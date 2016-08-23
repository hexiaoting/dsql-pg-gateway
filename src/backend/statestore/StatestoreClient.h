#ifndef STATESTORECLIENT_H
#define STATESTORECLIENT_H

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TTransportUtils.h>
#include <boost/foreach.hpp>
#include <iostream>
#include "StatestoreService.h"
#include "StatestoreService_types.h"
#include "StatestoreSubscriber.h"
#include "CatalogObjects_types.h"
#include "tool.h"
#include "thrift-server.h"

#define DSQLD_LENGTH 256
#define SINGLE_QEURY_SIZE 200
#define NAMEDATALEN 64

using namespace std;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace boost;
using namespace impala;

typedef struct DsqldNode
{
  int dsqldPort;
  char dsqldName[DSQLD_LENGTH];
  char dsqldIp[DSQLD_LENGTH];
  bool isValid;
} DsqldNode;

struct Meta {
  char dbname[NAMEDATALEN];
  char sub_query[SINGLE_QEURY_SIZE];
  struct Meta *next;
};

class StatestoreClient{
  public:
    friend class StatestoreSubscriberThriftIf;
    boost::shared_ptr<StatestoreSubscriberIf> thrift_iface_;
    boost::shared_ptr<ThriftServer> heartbeat_server_;
    StatestoreServiceClient *ss_client;
    boost::shared_ptr<TTransport> transport;

    typedef std::string TopicId;
    typedef std::map<TopicId, TTopicDelta> TopicDeltaMap;
    // Dsqld members cache
    typedef boost::unordered_map<std::string, TBackendDescriptor> BackendDescriptorMap;
    BackendDescriptorMap known_backends_;
    static int pg_port;
    static struct Meta *head;
    long lastSyncVersion_;
    vector<string> db_vec; //cache existed database
    int state;
    bool postMasterReady;

    StatestoreClient(char *ip, int port);

    void Register(int hb_port);

    void UpdateState(const TopicDeltaMap& incoming_topic_deltas);

    void Heartbeat();

    void UpdateCatalogTopicCallback(const TopicDeltaMap& incoming_topic_deltas);

    void UpdateDsqldMemberCallback(const TopicDeltaMap& incoming_topic_deltas);

    void ProcessDb(TDatabase db, bool add, char *db_query);

    void ProcessTable(TTable table, bool add, char *table_query);

    void ProcessIndex(TIndex index, bool add, char *index_query);

    void ProcessQuery(const char *dbname, char *query, int option);

    void Clear();

    void SetPort(int port);

    void WaitPostgresStart();

    struct Meta *FindOrInsertDb(const char *dbname);

    void SyncMeta();

    string ConvertType(int type);

    // sort all catalogobject in UpdateCatalogTopicCallback by catalog_version.
    // So make the metadata persistent in order.
    static bool CompareCatalogObject(std::pair<TCatalogObject, bool> object1, std::pair<TCatalogObject, bool> object2);

    void SortCatalogObject(vector<std::pair<TCatalogObject, bool> > &vec);

    void ClearDbVec(vector<string> &vec);

    void LoadDsqlNode(TBackendDescriptor backend, bool valid);
};

extern "C" {
  void start_subscribe(int pg_port, int ss_port, int hb_port, char *ss_host);
}
#endif
