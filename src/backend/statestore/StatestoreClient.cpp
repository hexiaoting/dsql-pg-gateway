#include "StatestoreClient.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <iostream>
#include <fstream>

#define SUBSCRIBER_ID "pg"
#define DSQLD_MEMBERSHIP_TOPIC "impala-membership"
#define CATALOG_UPDATE_TOPIC "catalog-update"

int StatestoreClient::pg_port = 5432;
struct Meta *StatestoreClient::head = NULL;
StatestoreClient* subscriber_client;

extern "C" {
  void dsql_create_context();
  void exec_dsql_query(const char *query_string, const char *db_name, int option);
  void dsql_process_metadata(struct Meta* head);
  void AddDsqldMember(DsqldNode *parsedNode);
  void ResetDsqldNodesHash();
}

void start_subscribe(int pg_port, int ss_port, int hb_port, char *ss_host) {
  cout << "subscriber port=" << pg_port << "ss_port=" << ss_port << "  hb_port=" << hb_port;
  subscriber_client = new StatestoreClient(ss_host, ss_port);
  subscriber_client->SetPort(pg_port);
  subscriber_client->Register(hb_port);
}

class StatestoreSubscriberThriftIf: public StatestoreSubscriberIf {
  private:
    StatestoreClient* subscriber_;
  public:
    StatestoreSubscriberThriftIf(StatestoreClient* subscriber)
      : subscriber_(subscriber) { }
    virtual void UpdateState(TUpdateStateResponse& response,
        const TUpdateStateRequest& params) {
      subscriber_->UpdateState(params.topic_deltas);
      // Make sure Thrift thinks the field is set.
      response.__set_skipped(false);
      response.status.status_code = TErrorCode::OK;
    }

    virtual void Heartbeat(THeartbeatResponse& response, const THeartbeatRequest& request) {
      subscriber_->Heartbeat();
    }
};

// Connect to statestored process on dsql host
StatestoreClient::StatestoreClient(char *ip, int port)
  : state(0),
    postMasterReady(false),
    lastSyncVersion_(0),
    thrift_iface_(new StatestoreSubscriberThriftIf(this)) {
  dsql_create_context();
  try {
    boost::shared_ptr<TTransport> socket(new TSocket(ip, port));
    transport = (boost::shared_ptr<TTransport>)new TBufferedTransport(socket);
    boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
    ss_client = new StatestoreServiceClient(protocol);
    transport->open();
  } catch (...)
  {
    cout << "StatestoreClient" << ip << " error.";
  }
}

void StatestoreClient::Heartbeat() {
}

void StatestoreClient::Register(int hb_port) {
  boost::shared_ptr<TProcessor> processor(new StatestoreSubscriberProcessor(thrift_iface_));

  // subscribe two topics: catalog-update and impala-membership
  TNetworkAddress subscriber_address = MakeNetworkAddress("localhost", hb_port);
  TRegisterSubscriberRequest request;
  request.topic_registrations.reserve(1);
  TTopicRegistration thrift_topic;
  thrift_topic.topic_name = CATALOG_UPDATE_TOPIC; // "catalog-update";
  thrift_topic.is_transient = false;
  TTopicRegistration thrift_topic2;
  thrift_topic2.topic_name = DSQLD_MEMBERSHIP_TOPIC;//"impala-membership";
  thrift_topic2.is_transient = false;
  request.topic_registrations.push_back(thrift_topic);
  request.topic_registrations.push_back(thrift_topic2);
  request.subscriber_location = subscriber_address;
  request.subscriber_id = SUBSCRIBER_ID;
  TRegisterSubscriberResponse response;
  ss_client->RegisterSubscriber(response, request);

  // Start Hearbeat process to be connected statestored later.
  heartbeat_server_.reset(new ThriftServer("StatestoreClient", processor, hb_port));
  heartbeat_server_->Start();
}

void StatestoreClient::UpdateState(const TopicDeltaMap& incoming_topic_deltas) {
  UpdateDsqldMemberCallback(incoming_topic_deltas);
  UpdateCatalogTopicCallback(incoming_topic_deltas);
}

void StatestoreClient::UpdateDsqldMemberCallback(
    const StatestoreClient::TopicDeltaMap& incoming_topic_deltas) {
  StatestoreClient::TopicDeltaMap::const_iterator topic =
    incoming_topic_deltas.find(DSQLD_MEMBERSHIP_TOPIC);
  if (topic == incoming_topic_deltas.end()) return ;

  const TTopicDelta& delta = topic->second;

  if (!delta.is_delta) {
    known_backends_.clear();
    ResetDsqldNodesHash();
  }

  // Process membership additions.
  BOOST_FOREACH(const TTopicItem& item, delta.topic_entries) {
    uint32_t len = item.value.size();
    TBackendDescriptor backend;
    DeserializeThriftMsg(reinterpret_cast<const uint8_t*>(
          item.value.data()), &len, false, &backend);
    // This is a new item - add it to the map of known backends.
    known_backends_.insert(make_pair(item.key, backend));
    LoadDsqlNode(backend, true);
  }
  //
  // Process membership deletions.
  BOOST_FOREACH(const string& backend_id, delta.topic_deletions) {
    BackendDescriptorMap::const_iterator tmp = known_backends_.find(backend_id);
    if (tmp == known_backends_.end())
      std::cout << "not found";
    else {
      LoadDsqlNode(tmp->second, false);
      known_backends_.erase(backend_id);
    }
  }
}

void StatestoreClient::LoadDsqlNode(TBackendDescriptor backend, bool valid) {
  DsqldNode *node = (DsqldNode *)malloc(sizeof(DsqldNode));
  if (node == NULL) {
    cout << "calloc DsqldNode failed." << endl;
    return ;
  }
  memset(node, 0, sizeof(DsqldNode));
  strcpy(node->dsqldName, backend.address.hostname.c_str());
  strcpy(node->dsqldIp, backend.ip_address.c_str());
  node->dsqldPort = backend.address.port;
  node->isValid = valid;
  cout << "hwt AddDsqldMember node=" << node->dsqldName << " valid=" << valid << endl;
  AddDsqldMember(node);
  free(node);
}

void StatestoreClient::UpdateCatalogTopicCallback(
    const StatestoreClient::TopicDeltaMap& incoming_topic_deltas) {
  if (state == 1) // Act as lock
    return;
  state = 1;
  long newCatalogVersion = 0;
  StatestoreClient::TopicDeltaMap::const_iterator topic =
    incoming_topic_deltas.find(CATALOG_UPDATE_TOPIC);//"catalog-update");
  if (topic == incoming_topic_deltas.end()) return ;

  const TTopicDelta& delta = topic->second;
  if (!delta.is_delta) {
    ClearDbVec(db_vec);
  }

  if (delta.topic_entries.size() == 0 &&  delta.topic_deletions.size() == 0) {
    state = 0;
    return;
  }
  cout << "\n###########hwt ---->UpdateCatalogTopicCallback lastSyncVersion_=" << lastSyncVersion_ << " "
    << delta.topic_entries.size() << " " << delta.topic_deletions.size() << endl;

  if (!postMasterReady) {
    WaitPostgresStart();
    postMasterReady = true;
  }

  vector<pair<TCatalogObject, bool> > vec;
  // Process all Catalog updates (new and modified objects) and determine what the
  // new catalog version will be.
  BOOST_FOREACH(const TTopicItem& item, delta.topic_entries) {
    uint32_t len = item.value.size();
    if (item.key == "catalogaddress") {
      continue;
    }

    TCatalogObject catalogObject;
    DeserializeThriftMsg(reinterpret_cast<const uint8_t*>(
          item.value.data()), &len, false, &catalogObject);
    vec.push_back(std::make_pair(catalogObject, true));
  }

  BOOST_FOREACH(const string& key, delta.topic_deletions) {
    TCatalogObject catalogObject;
    TCatalogObjectFromEntryKey(key, &catalogObject);
    vec.push_back(std::make_pair(catalogObject, false));
  }

  SortCatalogObject(vec);
  char *query = (char *)malloc(SINGLE_QEURY_SIZE);
  if (query == NULL) {
    cout << "malloc failed." << endl;
    return ;
  }
  for (int i = 0; i < vec.size(); i++) {
    TCatalogObject catalogObject = vec[i].first;
    if (vec[i].second && catalogObject.catalog_version <= lastSyncVersion_) {
      continue;
    }

    switch(catalogObject.type) {
      case TCatalogObjectType::CATALOG:
        newCatalogVersion = catalogObject.catalog_version;
        cout << "TCatalogObjectType::CATALOG newCatalogVersion = " << newCatalogVersion<< endl;
        break;
      case TCatalogObjectType::DATABASE:
        ProcessDb(catalogObject.db, vec[i].second, query);
        break;
      case TCatalogObjectType::INDEX:
        ProcessIndex(catalogObject.index, vec[i].second, query);
        break;
      case TCatalogObjectType::TABLE:
        ProcessTable(catalogObject.table, vec[i].second, query);
        break;
        //  case FUNCTION:
        //    dropFunction(catalogObject.getFn(), );
        //    break;
      default:
        cout << "Unexpected TCatalogObjectType in PG: " <<  catalogObject.type << endl;
    }
  }
  lastSyncVersion_ = newCatalogVersion;

  free(query);
  cout << "###########hwt <<----UpdateCatalogTopicCallback" << endl;
  state = 0;
}

void StatestoreClient::ProcessDb(TDatabase db, bool add, char *db_query) {
  memset(db_query, '\0', SINGLE_QEURY_SIZE);
  snprintf(db_query, SINGLE_QEURY_SIZE, "%s database %s", (add ? "create" : "drop"), db.db_name.c_str());
  if (add) db_vec.push_back(db.db_name);
  cout << db_query<< endl;
  exec_dsql_query(db_query, db.db_name.c_str(), add ? 1 : 11);
}


void StatestoreClient::ProcessTable(TTable table, bool add, char *table_query) {
  memset(table_query, '\0', SINGLE_QEURY_SIZE);
  strcat(table_query, add ? "create table ": "drop table ");
  strcat(table_query, table.tbl_name.c_str());
  if (add) {
    strcat(table_query, "(");
    vector<TColumn>  columns = table.columns;
    for (int i = 0; i < columns.size(); i++) {
      strcat(table_query, columns[i].columnName.c_str());
      strcat(table_query, " ");
      strcat(table_query, ConvertType(columns[i].columnType.types[0].scalar_type.type).c_str());
      if (i != columns.size() - 1)
        strcat(table_query, ",");
    }
    strcat(table_query, ")");
  }
  strcat(table_query, ";");
  ProcessQuery(table.db_name.c_str(), table_query, add ? 2 : 12);
}

void StatestoreClient::ProcessIndex(TIndex index, bool add, char *index_query) {
  memset(index_query, '\0', SINGLE_QEURY_SIZE);
  if (add) {
    snprintf(index_query, SINGLE_QEURY_SIZE, "create index %s on %s using %s (",
        index.idx_name.c_str(), index.tbl_name.c_str(), index.type.c_str());
    vector<TIndexColumnElement>  columns = index.columns;
    for (int i = 0; i < columns.size(); i++) {
      strcat(index_query, columns[i].name.c_str());
      if (i != columns.size() - 1)
        strcat(index_query, ",");
    }
    strcat(index_query, ")");
  } else {
    snprintf(index_query, SINGLE_QEURY_SIZE, "drop index %s", index.idx_name.c_str());
  }
  ProcessQuery(index.db_name.c_str(), index_query, add ? 3 : 13);
}

void StatestoreClient::ProcessQuery(const char *dbname, char *query, int option) {
  struct Meta *cur = FindOrInsertDb(dbname);
  strcpy(cur->sub_query, query);
  SyncMeta();
}

string StatestoreClient::ConvertType(int type) {
  switch(type) {
    case TPrimitiveType::BOOLEAN:
      return "boolean";
    case TPrimitiveType::SMALLINT:
      return "smallint";
    case TPrimitiveType::INT:
      return "integer";
    case TPrimitiveType::BIGINT:
      return "bigint";
    case TPrimitiveType::FLOAT:
      return "real";
    case TPrimitiveType::DOUBLE:
      return "double precision";
    case TPrimitiveType::TIMESTAMP:
      return "timestamp";
    case TPrimitiveType::STRING:
      return "text";
    case TPrimitiveType::DECIMAL:
      return "numeric";
    case TPrimitiveType::CHAR:
      return "character";
    case TPrimitiveType::VARCHAR:
      return "varchar";
    case TPrimitiveType::DATE:
      return "";
    case TPrimitiveType::DATETIME:
      return "";
    case TPrimitiveType::INVALID_TYPE:
    case TPrimitiveType::NULL_TYPE:
    case TPrimitiveType::TINYINT:
    case TPrimitiveType::BINARY:
      return "";
  }
}

struct Meta *StatestoreClient::FindOrInsertDb(const char *dbname) {
  struct Meta *cur = head;
  while (cur != NULL) {
    if (strcmp(cur->dbname, dbname) == 0)
      return cur;
    cur = cur->next;
  }
  cur = (struct Meta *)malloc(sizeof(struct Meta));
  if (cur == NULL)
    return NULL;
  memset(cur, 0, sizeof(struct Meta));
  strcpy(cur->dbname, dbname);
  cur->next = NULL;
  if (head == NULL)
    head = cur;
  else {
    cur->next = head;
    head = cur;
  }
  return cur;
}

void StatestoreClient::SyncMeta() {
  if (head == NULL)
    return ;
  dsql_process_metadata(head);
  struct Meta *cur = head;
  struct Meta *tmp = NULL;
  while(cur != NULL) {
    tmp = cur->next;
    free(cur);
    cur = tmp;
  }
  head = NULL;
}

void StatestoreClient::WaitPostgresStart() {
  int sock_fd = -1;
  struct sockaddr_in my_addr;
  memset(&my_addr, 0, sizeof(struct sockaddr_in));

  sock_fd = socket(AF_INET, SOCK_STREAM,0);
  if (sock_fd == -1) {
    exit(1);
  }
  my_addr.sin_family = AF_INET;
  my_addr.sin_port = htons(pg_port);
  my_addr.sin_addr.s_addr = inet_addr("127.0.0.1");
  while(connect(sock_fd, (struct sockaddr *)&my_addr, sizeof(struct sockaddr)) == -1) {
    cout << "Waiting for Postmaster ready..." << endl;
    sleep(2);
  }

  shutdown(sock_fd, SHUT_RDWR);
  close(sock_fd);
}

bool StatestoreClient::CompareCatalogObject(std::pair<TCatalogObject, bool> object1, std::pair<TCatalogObject, bool> object2) {
  return object1.first.catalog_version < object2.first.catalog_version;
}

void StatestoreClient::SortCatalogObject(vector<std::pair<TCatalogObject, bool> > &vec) {
  std::sort(vec.begin(), vec.end(), StatestoreClient::CompareCatalogObject);
}

void StatestoreClient::ClearDbVec(vector<string> &vec) {
  int size = vec.size();
  char query[SINGLE_QEURY_SIZE];
  for (int i = 0; i < size; i++) {
    memset(query, '\0', SINGLE_QEURY_SIZE);
    snprintf(query, SINGLE_QEURY_SIZE, "drop database %s", vec[i].c_str());
    exec_dsql_query(query, vec[i].c_str(), 11);
  }
  vec.clear();
}

void StatestoreClient::SetPort(int port) {
  pg_port = port;
}
