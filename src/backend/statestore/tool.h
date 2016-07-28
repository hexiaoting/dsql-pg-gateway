#ifndef TOOL_H
#define TOOL_H
#include <boost/algorithm/string.hpp>
#include <sstream>
#include <vector>

#include <boost/algorithm/string.hpp>
#include <boost/shared_ptr.hpp>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/protocol/TDebugProtocol.h>
#include <thrift/TApplicationException.h>
#include <thrift/transport/TBufferTransports.h>

#include <thrift/TDispatchProcessor.h>
#include <thrift/TProcessor.h>
#include <thrift/protocol/TProtocol.h>
#include "CatalogObjects_types.h"
using std::string;
using namespace std;

using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace apache::thrift;

using boost::algorithm::to_upper_copy;
using namespace boost;
using namespace impala;

shared_ptr<TProtocol> CreateDeserializeProtocol(
    shared_ptr<TMemoryBuffer> mem, bool compact) {
    TBinaryProtocolFactoryT<TMemoryBuffer> tproto_factory;
    return tproto_factory.getProtocol(mem);
}

template <class T>
bool static DeserializeThriftMsg(const uint8_t* buf, uint32_t* len, bool compact,
    T* deserialized_msg) {
  /// Deserialize msg bytes into c++ thrift msg using memory
  /// transport. TMemoryBuffer is not const-safe, although we use it in
  /// a const-safe way, so we have to explicitly cast away the const.
  boost::shared_ptr<apache::thrift::transport::TMemoryBuffer> tmem_transport(
      new apache::thrift::transport::TMemoryBuffer(const_cast<uint8_t*>(buf), *len));
  boost::shared_ptr<apache::thrift::protocol::TProtocol> tproto =
      CreateDeserializeProtocol(tmem_transport, compact);
  try {
    deserialized_msg->read(tproto.get());
  } catch (std::exception& e) {
    return false;
  } catch (...) {
    return false;
  }
  uint32_t bytes_left = tmem_transport->available_read();
  *len = *len - bytes_left;
  return true;
}

TCatalogObjectType::type TCatalogObjectTypeFromName(const string& name) {
  const string& upper = to_upper_copy(name);
  if (upper == "DATABASE") {
    return TCatalogObjectType::DATABASE;
  } else if (upper == "INDEX") {
    return TCatalogObjectType::INDEX;
  } else if (upper == "TABLE") {
    return TCatalogObjectType::TABLE;
  } else if (upper == "VIEW") {
    return TCatalogObjectType::VIEW;
  } else if (upper == "FUNCTION") {
    return TCatalogObjectType::FUNCTION;
  } else if (upper == "CATALOG") {
    return TCatalogObjectType::CATALOG;
  } else if (upper == "DATA_SOURCE") {
    return TCatalogObjectType::DATA_SOURCE;
  } else if (upper == "HDFS_CACHE_POOL") {
    return TCatalogObjectType::HDFS_CACHE_POOL;
  } else if (upper == "ROLE") {
    return TCatalogObjectType::ROLE;
  } else if (upper == "PRIVILEGE") {
    return TCatalogObjectType::PRIVILEGE;
  }
  return TCatalogObjectType::UNKNOWN;
}

bool TCatalogObjectFromObjectName(const TCatalogObjectType::type& object_type,
    const string& object_name, TCatalogObject* catalog_object) {
  switch (object_type) {
    case TCatalogObjectType::DATABASE:
      catalog_object->__set_type(object_type);
      catalog_object->__set_db(TDatabase());
      catalog_object->db.__set_db_name(object_name);
      break;
    case TCatalogObjectType::INDEX: {
      catalog_object->__set_type(object_type);
      catalog_object->__set_index(TIndex());
      // Parse what should be a fully qualified index name
      int pos = object_name.find(".");
      if (pos == string::npos || pos >= object_name.size() - 1) {
        return false;
      }
      catalog_object->index.__set_db_name(object_name.substr(0, pos));
      catalog_object->index.__set_idx_name(object_name.substr(pos + 1));
      break;
    }
    case TCatalogObjectType::TABLE: {
      catalog_object->__set_type(object_type);
      catalog_object->__set_table(TTable());
      // Parse what should be a fully qualified table name
      int pos = object_name.find(".");
      if (pos == string::npos || pos >= object_name.size() - 1) {
        return false;
      }
      catalog_object->table.__set_db_name(object_name.substr(0, pos));
      catalog_object->table.__set_tbl_name(object_name.substr(pos + 1));
      break;
    }
    case TCatalogObjectType::FUNCTION: {
      // The key looks like: <db>.fn(<args>). We need to parse out the
      // db, fn and signature.
      catalog_object->__set_type(object_type);
      catalog_object->__set_fn(TFunction());
      int dot = object_name.find(".");
      int paren = object_name.find("(");
      if (dot == string::npos || dot >= object_name.size() - 1 ||
          paren == string::npos || paren >= object_name.size() - 1 ||
          paren <= dot) {
        return false;
      }
      catalog_object->fn.name.__set_db_name(object_name.substr(0, dot));
      catalog_object->fn.name.__set_function_name(
          object_name.substr(dot + 1, paren - dot - 1));
      catalog_object->fn.__set_signature(object_name.substr(dot + 1));
      break;
    }
    case TCatalogObjectType::CATALOG:
    case TCatalogObjectType::UNKNOWN:
    default:
      return false;
  }
  return true;
}

bool static TCatalogObjectFromEntryKey(const string& key,
    TCatalogObject* catalog_object) {
  // Reconstruct the object based only on the key.
  size_t pos = key.find(":");
  if (pos == string::npos || pos >= key.size() - 1) {
    return false;
  }

  TCatalogObjectType::type object_type = TCatalogObjectTypeFromName(key.substr(0, pos));
  string object_name = key.substr(pos + 1);
  return TCatalogObjectFromObjectName(object_type, object_name, catalog_object);
}

TNetworkAddress MakeNetworkAddress(const string& address) {
  vector<string> tokens;
  split(tokens, address, is_any_of(":"));
  TNetworkAddress ret;
  if (tokens.size() == 1) {
    ret.__set_hostname(tokens[0]);
    ret.port = 0;
    return ret;
  }
  if (tokens.size() != 2) return ret;
  ret.__set_hostname(tokens[0]);
//  StringParser::ParseResult parse_result;
//  int32_t port = StringParser::StringToInt<int32_t>(
//      tokens[1].data(), tokens[1].length(), &parse_result);
//  if (parse_result != StringParser::PARSE_SUCCESS) return ret;
  int32_t port = atoi(tokens[1].data());
  ret.__set_port(port);
  return ret;
}

TNetworkAddress MakeNetworkAddress(const string& hostname, int port) {
  TNetworkAddress ret;
  ret.__set_hostname(hostname);
  ret.__set_port(port);
  return ret;
}

#endif
