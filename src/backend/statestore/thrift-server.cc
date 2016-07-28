// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <boost/filesystem.hpp>
#include <boost/thread.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/condition_variable.hpp>
#include <boost/uuid/uuid_io.hpp>

#include <thrift/concurrency/Thread.h>
#include <thrift/concurrency/ThreadManager.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TThreadedServer.h>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TSSLServerSocket.h>
#include <thrift/transport/TSSLSocket.h>
#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TBufferTransports.h>

#include "Types_types.h"
#include "thrift-server.h"
#include <sstream>

using boost::filesystem::exists;
using boost::get_system_time;
using boost::system_time;
using boost::uuids::uuid;
using namespace apache::thrift::concurrency;
using namespace apache::thrift::protocol;
using namespace apache::thrift::server;
using namespace apache::thrift::transport;
using namespace apache::thrift;
using namespace std;
using namespace boost;

namespace impala {
  ThriftServer::ThriftServer(const std::string& name, boost::shared_ptr<apache::thrift::TProcessor>& processor, int port)
    : port_(port),
      name_(name),
      server_thread_(NULL),
      server_(NULL),
      processor_(processor)
      {}

bool ThriftServer::Start() {
  shared_ptr<TProtocolFactory> protocol_factory(new TBinaryProtocolFactory());

  shared_ptr<TServerTransport> server_socket(new TServerSocket(port_));
  shared_ptr<TTransportFactory> transport_factory(new TBufferedTransportFactory());

  TThreadedServer server(processor_, server_socket, transport_factory, protocol_factory);
  cout << "server.server()" << endl;
  server.serve();

  return true;
}

}
