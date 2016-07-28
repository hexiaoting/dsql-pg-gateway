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

using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;

using namespace std;
using namespace boost;
using namespace beeswax;

class DsqlClient{
protected:
  BeeswaxServiceClient *client;
  boost::shared_ptr<TTransport> transport;
  QueryHandle qhandle;

public:
  DsqlClient(char *ip, int port) {
    try {
      boost::shared_ptr<TTransport> socket(new TSocket(ip, port));
      transport = (boost::shared_ptr<TTransport>)new TBufferedTransport(socket);
      boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
      client = new BeeswaxServiceClient(protocol);
      transport->open();
    } catch (...)
    {

    }
  }

  int wait_to_finish()
  {
    while (true)
    {
      QueryState::type state = client->get_state(qhandle);
      if (state == (QueryState::FINISHED))
        return 0;
      else if (state == (QueryState::EXCEPTION))
      {
        return 1;
      }
    }
  }
  Results fetch_result()
  {
    int fetch_size = 10000;
    Results result ;
    bool first = true;
    // last parameter stands for Resultset_format is Binary
    while (true)
    {
      Results tmp;
      client->fetch(tmp, qhandle, false, fetch_size);
      if (first)
      {
        first = false;
        result = tmp;
      }
      else
      {
        for (int i = 0; i < tmp.data.size(); i++)
          result.data.push_back(tmp.data[i]);
      }
      if (tmp.has_more == false)
      {
        return result;
      }
    }
  }

  char *send_query(char* query_string) {
    try {
      Query q;
      q.__set_query(query_string);
      q.__set_hadoop_user("root");
      client->query(qhandle, q);
      wait_to_finish();
      return NULL;
    }  catch (const TException& e) {
      cerr <<  e.what();
      return (char *)(e.what());
    }

  }

  void get_results_metadata(ResultsMetadata& meta) {
    client->get_results_metadata(meta, qhandle);
  }

  void close_connection() {
    transport->close();
  }
};

extern "C" {
struct UpdateDsqldMember {
  char hostname[20];
  char ip_address[20];
  int port;
  struct UpdateDsqldMember *next;
};

DsqlClient *client;
ResultsMetadata meta;
Results res;
int nattrs = -1;
int rowCount = -1;
int idx = 0;
static struct UpdateDsqldMember *dsqlds = NULL;

void reset_dsqld_members(struct UpdateDsqldMember *head)
{
  struct UpdateDsqldMember *tmp = NULL;
  struct UpdateDsqldMember *cur= dsqlds;
  while (cur != NULL) {
    tmp = cur->next;
    free(cur);
    cur = tmp;
  }
  dsqlds = head;
}

char *getRandomDsqlIp()
{
  if (dsqlds != NULL)
    return dsqlds->ip_address;
  return "localhost";
}

void connect_dsqld()
{
  char *ip = getRandomDsqlIp();
  int port = 21000;
  client = new DsqlClient(ip, port);
}
char *request_remote(char *request)
{
  idx = 0;
  nattrs = -1;
  rowCount = -1;
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

int get_cols_num()
{
  if (nattrs == -1){
    get_meta_info();
  }
  return nattrs;
}


bool get_column_name(char *colName) {
  if (nattrs == -1) {
    get_meta_info();
  }
  //TODO : the size of col_name < 32
  for (int i = 0; i < nattrs; i++) {
    if (strlen((char *)(meta.schema.fieldSchemas.at(i).name.c_str())) > 31)
      return false;
    strcpy(&colName[i * 32], (char *)(meta.schema.fieldSchemas.at(i).name.c_str()));
  }
  return true;
}

bool get_type_name(char *colType) {
  res = client->fetch_result();
  for (int i = 0; i < res.columns.size(); i++) {
    if (strlen((char *)(res.columns.at(i).c_str())) > 19)
      return false;
    strcpy(&colType[i * 20], (char *)(res.columns.at(i).c_str()));
  }
  rowCount = res.data.size();
  return true;
}

char **get_cols_name(int *num)
{
  client->get_results_metadata(meta);
  ::Apache::Hadoop::Hive::Schema& schema = meta.schema;
  *num = schema.fieldSchemas.size();
  char **cols = (char **)malloc(sizeof(char *) * (*num));
  if (cols == NULL)
    return NULL;
  for (int i = 0; i < *num; i++) {
    cols[i] = (char *)(schema.fieldSchemas.at(i).name.c_str());
  }
  return cols;
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

void disconnect_dsqld()
{
  client->close_connection();
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

}
