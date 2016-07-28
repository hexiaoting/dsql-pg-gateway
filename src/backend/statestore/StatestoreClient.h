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
#define HEARTBEAT_PORT 23021


using namespace std;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace boost;
using namespace impala;

struct UpdateDsqldMember {
  char hostname[20];
  char ip_address[20];
  int port;
  struct UpdateDsqldMember *next;
};

class StatestoreClient{
  public:
    typedef std::string TopicId;
    friend class StatestoreSubscriberThriftIf;
    boost::shared_ptr<StatestoreSubscriberIf> thrift_iface_;
    boost::shared_ptr<ThriftServer> heartbeat_server_;
    StatestoreServiceClient *ss_client;
    boost::shared_ptr<TTransport> transport;
    static char *pg_port;

    // Dsqld members
    typedef boost::unordered_map<std::string, TBackendDescriptor> BackendDescriptorMap;
    BackendDescriptorMap known_backends_;

    StatestoreClient(char *ip, int port);

    typedef std::map<TopicId, TTopicDelta> TopicDeltaMap;

    void Register();

    void UpdateState(const TopicDeltaMap& incoming_topic_deltas);

    void processDb(TDatabase db, bool add, char *db_query);

    void processTable(TTable table, bool add, char *table_query);

    void processIndex(TIndex index, bool add, char *index_query);

    void Heartbeat();

    void UpdateCatalogTopicCallback(const TopicDeltaMap& incoming_topic_deltas);

    void UpdateDsqldMemberCallback(const TopicDeltaMap& incoming_topic_deltas);

    void clear();

    void process_query(const char *dbname, char *query);

    void SetPort(char *port);

    void wait_postgres_start();

};
extern "C" {
  void start_subscribe(char *port);
}
#endif
