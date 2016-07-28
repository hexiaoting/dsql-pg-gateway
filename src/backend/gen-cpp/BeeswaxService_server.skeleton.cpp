// This autogenerated skeleton file illustrates how to build a server.
// You should copy it to another filename to avoid overwriting it.

#include "BeeswaxService.h"
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TSimpleServer.h>
#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TBufferTransports.h>

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;

using boost::shared_ptr;

using namespace  ::beeswax;

class BeeswaxServiceHandler : virtual public BeeswaxServiceIf {
 public:
  BeeswaxServiceHandler() {
    // Your initialization goes here
  }

  void query(QueryHandle& _return, const Query& query) {
    // Your implementation goes here
    printf("query\n");
  }

  void executeAndWait(QueryHandle& _return, const Query& query, const LogContextId& clientCtx) {
    // Your implementation goes here
    printf("executeAndWait\n");
  }

  void explain(QueryExplanation& _return, const Query& query) {
    // Your implementation goes here
    printf("explain\n");
  }

  void fetch(Results& _return, const QueryHandle& query_id, const bool start_over, const int32_t fetch_size) {
    // Your implementation goes here
    printf("fetch\n");
  }

  QueryState::type get_state(const QueryHandle& handle) {
    // Your implementation goes here
    printf("get_state\n");
  }

  void get_results_metadata(ResultsMetadata& _return, const QueryHandle& handle) {
    // Your implementation goes here
    printf("get_results_metadata\n");
  }

  void echo(std::string& _return, const std::string& s) {
    // Your implementation goes here
    printf("echo\n");
  }

  void dump_config(std::string& _return) {
    // Your implementation goes here
    printf("dump_config\n");
  }

  void get_log(std::string& _return, const LogContextId& context) {
    // Your implementation goes here
    printf("get_log\n");
  }

  void get_default_configuration(std::vector<ConfigVariable> & _return, const bool include_hadoop) {
    // Your implementation goes here
    printf("get_default_configuration\n");
  }

  void close(const QueryHandle& handle) {
    // Your implementation goes here
    printf("close\n");
  }

  void clean(const LogContextId& log_context) {
    // Your implementation goes here
    printf("clean\n");
  }

};

int main(int argc, char **argv) {
  int port = 9090;
  shared_ptr<BeeswaxServiceHandler> handler(new BeeswaxServiceHandler());
  shared_ptr<TProcessor> processor(new BeeswaxServiceProcessor(handler));
  shared_ptr<TServerTransport> serverTransport(new TServerSocket(port));
  shared_ptr<TTransportFactory> transportFactory(new TBufferedTransportFactory());
  shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());

  TSimpleServer server(processor, serverTransport, transportFactory, protocolFactory);
  server.serve();
  return 0;
}

