#include "StatestoreClient.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <iostream>
#include <fstream>
#define QEURY_SIZE 200
#include "libpq-fe.h"

extern "C" {
  void exec_dsql_query(const char *query_string, const char *db_name);
  void reset_dsqld_members(struct UpdateDsqldMember *head);
}

class StatestoreSubscriberThriftIf: public StatestoreSubscriberIf{
  private:
    StatestoreClient* subscriber_;
  public:
    StatestoreSubscriberThriftIf(StatestoreClient* subscriber)
      : subscriber_(subscriber) {  }
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

StatestoreClient::StatestoreClient(char *ip, int port):
  thrift_iface_(new StatestoreSubscriberThriftIf(this)) {
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

int StatestoreClient::pg_port = 5432;
void StatestoreClient::Register(int hb_port) {
  boost::shared_ptr<TProcessor> processor(new StatestoreSubscriberProcessor(thrift_iface_));

  TNetworkAddress subscriber_address =  MakeNetworkAddress("localhost", hb_port);
  TRegisterSubscriberRequest request;
  request.topic_registrations.reserve(1);
  TTopicRegistration thrift_topic;
  thrift_topic.topic_name =  "catalog-update";
  thrift_topic.is_transient = false;
  TTopicRegistration thrift_topic2;
  thrift_topic2.topic_name =  "impala-membership";
  thrift_topic2.is_transient = false;
  request.topic_registrations.push_back(thrift_topic);
  request.topic_registrations.push_back(thrift_topic2);

  request.subscriber_location = subscriber_address;
  request.subscriber_id = "pg";
  TRegisterSubscriberResponse response;
  ss_client->RegisterSubscriber(response, request);
  heartbeat_server_.reset(new ThriftServer("StatestoreClient", processor, hb_port));
  cout << "start thread " << endl;
  heartbeat_server_->Start();
}

void StatestoreClient::UpdateState(const TopicDeltaMap& incoming_topic_deltas) {
  UpdateDsqldMemberCallback(incoming_topic_deltas);
  UpdateCatalogTopicCallback(incoming_topic_deltas);
}

void StatestoreClient::processDb(TDatabase db, bool add, char *db_query) {
  memset(db_query, '\0', QEURY_SIZE);
  strcpy(db_query, "!@#");
  strcat(db_query, add ? "create database " : "drop database ");
  strcat(db_query, db.db_name.c_str());
  process_query("template1", db_query);
}

char *convert_type(int type)
{
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

void StatestoreClient::process_query(const char *dbname, char *query) {
  PGresult   *res;
  stringstream conn_str;
  conn_str << "dbname = " << dbname << " port = " << pg_port;
  string conn_info = conn_str.str();

  PGconn *conn = PQconnectdb((const char *)(conn_info.c_str()));
  if (PQstatus(conn) != CONNECTION_OK) {
    cout << "connect " << conn_info  << " failed." << endl;
    PQfinish(conn);
    return ;
  }

  res = PQexec(conn, query);
  if (PQresultStatus(res) != PGRES_COMMAND_OK) {
    cout << "query=" << query << "  failed." << endl;
  }
  PQclear(res);
  PQfinish(conn);
}

void StatestoreClient::processTable(TTable table, bool add, char *table_query) {
  memset(table_query, '\0', QEURY_SIZE);
  strcpy(table_query, "!@#");
  strcat(table_query, add ? "create table ": "drop table ");
  strcat(table_query, table.tbl_name.c_str());
  //cout << add << " columns:" << table.columns.size() << "table_type:" << table.table_type << endl;
  if (add) {
    strcat(table_query, "(");
    vector<TColumn>  columns = table.columns;
    for (int i = 0; i < columns.size(); i++) {
      strcat(table_query, columns[i].columnName.c_str());
      strcat(table_query, " ");
      strcat(table_query, convert_type(columns[i].columnType.types[0].scalar_type.type));
      if (i != columns.size() - 1)
        strcat(table_query, ",");
    }
    strcat(table_query, ")");
  }
  cout << "before process_query table_query=" << table_query << endl;
  process_query(table.db_name.c_str(), table_query);
}

void StatestoreClient::processIndex(TIndex index, bool add, char *index_query) {
  memset(index_query, '\0', QEURY_SIZE);
  strcpy(index_query, "!@#");
  strcat(index_query, add ? "create index ": "drop index ");
  strcat(index_query, index.idx_name.c_str());
  if (add) {
    strcat(index_query, " on ");
    strcat(index_query, index.tbl_name.c_str());
    strcat(index_query, " using ");
    strcat(index_query, index.type.c_str());
    strcat(index_query, "(");
    vector<TIndexColumnElement>  columns = index.columns;
    for (int i = 0; i < columns.size(); i++) {
      strcat(index_query, columns[i].name.c_str());
      if (i != columns.size() - 1)
        strcat(index_query, ",");
    }
    strcat(index_query, ")");
  }
  cout << index_query << endl;
  process_query(index.db_name.c_str(), index_query);
}

void StatestoreClient::UpdateDsqldMemberCallback(
    const StatestoreClient::TopicDeltaMap& incoming_topic_deltas) {
  StatestoreClient::TopicDeltaMap::const_iterator topic =
    incoming_topic_deltas.find("impala-membership");
  if (topic == incoming_topic_deltas.end()) return ;

  const TTopicDelta& delta = topic->second;

  if (!delta.is_delta) known_backends_.clear();

  // Process membership additions.
  BOOST_FOREACH(const TTopicItem& item, delta.topic_entries) {
    uint32_t len = item.value.size();
    TBackendDescriptor backend_descriptor;
    DeserializeThriftMsg(reinterpret_cast<const uint8_t*>(
          item.value.data()), &len, false, &backend_descriptor);
    // This is a new item - add it to the map of known backends.
    known_backends_.insert(make_pair(item.key, backend_descriptor));
  }
  // Process membership deletions.
  BOOST_FOREACH(const string& backend_id, delta.topic_deletions) {
    known_backends_.erase(backend_id);
  }

  // Create a set of known backend network addresses. Used to test for cluster
  // membership by network address.
  // Also reflect changes to the frontend. Initialized only if any_changes is true.
  struct UpdateDsqldMember *head = NULL;
  struct UpdateDsqldMember *update_req = NULL;
  bool any_changes = !delta.topic_entries.empty() || !delta.topic_deletions.empty() ||
    !delta.is_delta;
  BOOST_FOREACH(const BackendDescriptorMap::value_type& backend, known_backends_) {
    if (any_changes) {
      update_req = (struct UpdateDsqldMember *)malloc(sizeof(struct UpdateDsqldMember));
      if (update_req == NULL)
        return ;
      update_req->next = head;
      memset(update_req->hostname, '\0', 20);
      strcpy(update_req->hostname, backend.second.address.hostname.c_str());
      memset(update_req->ip_address, '\0', 20);
      strcpy(update_req->ip_address, backend.second.ip_address.c_str());
      head = update_req;
    }
  }
  if (any_changes) {
    cout << "reset_dsqld_members head=" << head->ip_address;
    reset_dsqld_members(head);
  }
}

void StatestoreClient::wait_postgres_start() {
  //TODO hwt: PQping
  stringstream file_name;
  file_name << "/tmp/.s.PGSQL." << pg_port;
  string path = file_name.str();
  struct stat buf;
  while(-1 == stat(path.c_str(), &buf)) {
    cout << "port " << pg_port << " isnot ready";
    sleep(2);
  }

  /*
  int port = atoi(pg_port);
  int sock_fd = -1;
  struct sockaddr_in my_addr;
  memset(&my_addr, 0, sizeof(struct sockaddr_in));

  sock_fd = socket(AF_INET, SOCK_STREAM,0);
  if (sock_fd == -1) {
    exit(1);
  }
  my_addr.sin_family = AF_INET;
  my_addr.sin_port = port;
  my_addr.sin_addr.s_addr = inet_addr("127.0.0.1");
  while(connect(sock_fd, (struct sockaddr *)&my_addr, sizeof(struct sockaddr)) == -1) {
    sleep(2);
  }

  shutdown(sock_fd, SHUT_RDWR);
  close(sock_fd);
  */
}

void StatestoreClient::UpdateCatalogTopicCallback(
    const StatestoreClient::TopicDeltaMap& incoming_topic_deltas) {
  StatestoreClient::TopicDeltaMap::const_iterator topic =
    incoming_topic_deltas.find("catalog-update");
  if (topic == incoming_topic_deltas.end()) return ;

  const TTopicDelta& delta = topic->second;

  if (delta.topic_entries.size() == 0 &&  delta.topic_deletions.size() == 0)
    return ;

  wait_postgres_start();
  char *query = (char *)malloc(QEURY_SIZE);
  if (query == NULL) {
    cout << "malloc failed." << endl;
    return ;
  }
  // Process all Catalog updates (new and modified objects) and determine what the
  // new catalog version will be.
  // BOOST_FOREACH(const TTopicItem& item, delta.topic_entries) {
  for (int i = 0; i < delta.topic_entries.size(); i++) {
    const TTopicItem& item = delta.topic_entries.at(i);
    uint32_t len = item.value.size();

    if (item.key == "catalogaddress") {
      continue;
    }

    TCatalogObject catalogObject;
    DeserializeThriftMsg(reinterpret_cast<const uint8_t*>(
          item.value.data()), &len, false, &catalogObject);

    switch(catalogObject.type) {
      case TCatalogObjectType::DATABASE:
        processDb(catalogObject.db, true, query);
        break;
      case TCatalogObjectType::INDEX:
        processIndex(catalogObject.index, true, query);
        break;
      case TCatalogObjectType::TABLE:
        processTable(catalogObject.table, true, query);
        break;
        // case FUNCTION:
        //   addFunction(catalogObject.getFn(), );
        //   break;
      default:
        cout << "Unexpected TCatalogObjectType in PG: " <<  catalogObject.type << endl;
    }
  }


  // Process all Catalog deletions (dropped objects). We only know the keys (object
  // names) so must parse each key to determine the TCatalogObject.
  BOOST_FOREACH(const string& key, delta.topic_deletions) {
    TCatalogObject catalogObject;
    TCatalogObjectFromEntryKey(key, &catalogObject);
    switch(catalogObject.type) {
      case TCatalogObjectType::DATABASE:
        processDb(catalogObject.db, false, query);
        break;
      case TCatalogObjectType::INDEX:
        processIndex(catalogObject.index, false, query);
        break;
      case TCatalogObjectType::TABLE:
        processTable(catalogObject.table, false, query);
        break;
        //  case FUNCTION:
        //    dropFunction(catalogObject.getFn(), );
        //    break;
      default:
        cout << "Unexpected TCatalogObjectType in PG: " <<  catalogObject.type << endl;
    }
  }
  free(query);
  return ;
}

void StatestoreClient::SetPort(int port)
{
  pg_port = port;
}

void StatestoreClient::Heartbeat()
{
}

StatestoreClient* subscriber_client;
void start_subscribe(int pg_port, int ss_port, int hb_port, char *ss_host)
{
  cout << "subscriber port=" << pg_port << "ss_port=" << ss_port << "  hb_port=" << hb_port;
  subscriber_client = new StatestoreClient(ss_host, ss_port);
  subscriber_client->SetPort(pg_port);
  subscriber_client->Register(hb_port);
}
