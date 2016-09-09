#include <iostream>
#include <boost/algorithm/string.hpp>
#include <string>
#include <vector>
#include <stdlib.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TTransportUtils.h>
#include "BeeswaxService.h"
#include "beeswax_types.h"
#include "ImpalaService.h"
#include "ImpalaService_types.h"

#define DSQLD_LENGTH 256 // dsql hostname and ip max length
#define NAMEDATALEN 64   // The max size for tableName/colName/functionName
                         // defined in pg_config_manual.h
#define COLTYPELEN  20   // The max size for column type
#define FETCHCOUNT 10000

using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace std;
using namespace boost;
using namespace beeswax;
using namespace impala;

class DsqlClient{
protected:
  ImpalaServiceClient *client;
  boost::shared_ptr<TTransport> transport;
  QueryHandle qhandle;

public:
  DsqlClient(char *ip, int port) {
    // Connect to dsqld host
    try {
      boost::shared_ptr<TTransport> socket(new TSocket(ip, port));
      transport = (boost::shared_ptr<TTransport>)new TBufferedTransport(socket);
      boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
      client = new ImpalaServiceClient(protocol);
      transport->open();
    } catch (...)
    {
      cout << " new DsqlClient failed.****************";
    }
  }

  // results:
  //  0   finished
  //  -1  exception
  int wait_to_finish()
  {
    while (true)
    {
      QueryState::type state = client->get_state(qhandle);
      if (state == QueryState::FINISHED)
        return 0;
      else if (state == QueryState::EXCEPTION) {
        return -1;
      }
      sleep(1);
    }
  }

  Results fetch_result()
  {
    Results   result;
    client->fetch(result, qhandle, false, FETCHCOUNT);
    if (result.has_more) {
      while (true) {
        Results tmp;
        client->fetch(tmp, qhandle, false, FETCHCOUNT);
        for (int i = 0; i < tmp.data.size(); i++)
          result.data.push_back(tmp.data[i]);
        if (!(tmp.has_more)) {
          return result;
        }
      }
    }
  }

  char *get_warning_log() {
    string result;
    client->get_log(result, qhandle.log_context);
    return (char *)(result.c_str());
  }

  char *send_query(char* query_string) {
    try {
      Query q;
      q.__set_query(query_string);
      q.__set_hadoop_user("root");
      client->query(qhandle, q);
      if(wait_to_finish() == -1)
        return get_warning_log();
      return NULL;
    }  catch (const TException& e) {
      return (char *)(e.what());
    }
  }

  void get_results_metadata(ResultsMetadata& meta) {
    client->get_results_metadata(meta, qhandle);
  }

  void close_connection() {
    transport->close();
  }

  int64_t close_insert() {
    int64_t rows = 0;
    TInsertResult result;
    client->CloseInsert(result, qhandle);
    map <string, int64_t>::iterator it;
    for (it = result.rows_appended.begin( ); it != result.rows_appended.end( ); it++ ) {
      rows += it->second;
    }
    return rows;
  }
};

extern "C" {
typedef struct DsqldNode
{
  int  dsqldPort;
  char dsqldName[DSQLD_LENGTH];
  char dsqldIp[DSQLD_LENGTH];
  bool isValid;
} DsqldNode;

DsqlClient      *client;
ResultsMetadata meta;
Results         res;
int             nattrs = -1;   // column num
int             rowCount = -1; // select result: row num
int             idx = 0;

char *request_remote(char *request)
{
  idx       =   0;
  nattrs    =   -1;
  rowCount  =   -1;
  //TODO: add exception
  return client->send_query(request);
}

void get_meta_info()
{
  try {
    client->get_results_metadata(meta);
    ::Apache::Hadoop::Hive::Schema& schema = meta.schema;
    nattrs = schema.fieldSchemas.size();
  }  catch (const TException& e) {
    cerr <<  e.what();
  }
}

int get_col_num()
{
  if (nattrs == -1){
    get_meta_info();
  }
  return nattrs;
}

bool get_col_name(char *colName) {
  if (nattrs == -1) {
    get_meta_info();
  }
  for (int i = 0; i < nattrs; i++) {
    if (strlen((char *)(meta.schema.fieldSchemas.at(i).name.c_str())) > (NAMEDATALEN - 1))
      return false;
    strcpy(&colName[i * NAMEDATALEN], (char *)(meta.schema.fieldSchemas.at(i).name.c_str()));
  }
  return true;
}

bool get_type_name(char *colType) {
  res = client->fetch_result();
  for (int i = 0; i < res.columns.size(); i++) {
    if (strlen((char *)(res.columns.at(i).c_str())) > (COLTYPELEN - 1))
      return false;
    strcpy(&colType[i * COLTYPELEN], (char *)(res.columns.at(i).c_str()));
  }
  rowCount = res.data.size();
  return true;
}

int get_line(int length, char *data) {
  if (idx >= rowCount) {
    data = NULL;
    return 0;
  }
  string line = res.data.at(idx);
  if (line.size() < length) {
    idx++;
    int i = 0;
    // replace \t to \0
    for (i = 0; line[i] != '\0'; i++)
      data[i] = (line[i] == '\t' ? '\0' : line[i]);
    data[i] = '\0';
  }
  return line.size() + 1;
}

void convert(char *data, int length, char **values, int nattrs) {
  int i = 0, idx = 0;
  values[idx] = data;
  if (strcmp(values[idx], "NULL") == 0)
    values[idx] = NULL;
  idx++;
  for (i = 0; i < length; i++)
  {
    if (data[i] == '\0' && idx < nattrs) {
      values[idx] = data + i + 1;
      if (strcmp(values[idx], "NULL") == 0)
        values[idx] = NULL;
      idx++;
    }
  }
}

long extract_num(const char *num_str)
{
  char num[20];
  int idx = 0, i = 8;
  if (strncmp(num_str, "Deleted", 7) != 0 && strncmp(num_str, "Updated", 7) != 0) {
    cout << "extract wrong result" << num_str <<  endl;
    return -1;
  }
  while(num_str[i] != ' ') {
    num[idx++] = num_str[i++];
  }
  num[idx++] = '\0';
  return atol(num);
}

long get_deletedUpdated_rows()
{
  const char* num_str = NULL;

  res = client->fetch_result();
  num_str = res.data[0].c_str();
  return extract_num(num_str);
}

int get_inserted_rows()
{
  return client->close_insert();
}

void connect_dsqld(char dsqldHostname[], int port)
{
  cout << "@@@@ connect dsqld to send query:" << dsqldHostname << endl;
  client = new DsqlClient(dsqldHostname, port);
}

void disconnect_dsqld()
{
  client->close_connection();
}
}
