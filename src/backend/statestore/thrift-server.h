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


#ifndef IMPALA_RPC_THRIFT_SERVER_H
#define IMPALA_RPC_THRIFT_SERVER_H

#include <boost/thread/mutex.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/unordered_map.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <thrift/server/TServer.h>
#include <thrift/TProcessor.h>
#include <thrift/concurrency/Thread.h>

using namespace apache::thrift::concurrency;

namespace impala {

/// Utility class for all Thrift servers. Runs a threaded server by default, or a
/// TThreadPoolServer with, by default, 2 worker threads, that exposes the interface
/// described by a user-supplied TProcessor object.
/// If TThreadPoolServer is used, client must use TSocket as transport.
/// TODO: Need a builder to help with the unwieldy constructor
/// TODO: shutdown is buggy (which only harms tests)
class ThriftServer {
 public:
  /// Username.
  typedef std::string Username;

  ThriftServer(const std::string& name,
      boost::shared_ptr<apache::thrift::TProcessor>& processor, int port);

  int port() const { return port_; }

  bool Start();

 private:
  /// The port on which the server interface is exposed
  int port_;

  /// User-specified identifier that shows up in logs
  const std::string name_;

  /// Thread that runs ThriftServerEventProcessor::Supervise() in a separate loop
  boost::scoped_ptr<Thread> server_thread_;

  /// Thrift housekeeping
  boost::scoped_ptr<apache::thrift::server::TServer> server_;
  boost::shared_ptr<apache::thrift::TProcessor> processor_;

  /// Helper class which monitors starting servers. Needs access to internal members, and
  /// is not used outside of this class.
  class ThriftServerEventProcessor;
  friend class ThriftServerEventProcessor;
};

// Returns true if, per the process configuration flags, server<->server communications
// should use SSL.
bool EnableInternalSslConnections();
}

#endif
