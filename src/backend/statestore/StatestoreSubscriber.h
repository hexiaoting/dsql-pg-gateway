/**
 * Autogenerated by Thrift Compiler (0.9.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
#ifndef StatestoreSubscriber_H
#define StatestoreSubscriber_H

#include <thrift/TDispatchProcessor.h>
#include "StatestoreService_types.h"

namespace impala {

class StatestoreSubscriberIf {
 public:
  virtual ~StatestoreSubscriberIf() {}
  virtual void UpdateState(TUpdateStateResponse& _return, const TUpdateStateRequest& params) = 0;
  virtual void Heartbeat(THeartbeatResponse& _return, const THeartbeatRequest& params) = 0;
};

class StatestoreSubscriberIfFactory {
 public:
  typedef StatestoreSubscriberIf Handler;

  virtual ~StatestoreSubscriberIfFactory() {}

  virtual StatestoreSubscriberIf* getHandler(const ::apache::thrift::TConnectionInfo& connInfo) = 0;
  virtual void releaseHandler(StatestoreSubscriberIf* /* handler */) = 0;
};

class StatestoreSubscriberIfSingletonFactory : virtual public StatestoreSubscriberIfFactory {
 public:
  StatestoreSubscriberIfSingletonFactory(const boost::shared_ptr<StatestoreSubscriberIf>& iface) : iface_(iface) {}
  virtual ~StatestoreSubscriberIfSingletonFactory() {}

  virtual StatestoreSubscriberIf* getHandler(const ::apache::thrift::TConnectionInfo&) {
    return iface_.get();
  }
  virtual void releaseHandler(StatestoreSubscriberIf* /* handler */) {}

 protected:
  boost::shared_ptr<StatestoreSubscriberIf> iface_;
};

class StatestoreSubscriberNull : virtual public StatestoreSubscriberIf {
 public:
  virtual ~StatestoreSubscriberNull() {}
  void UpdateState(TUpdateStateResponse& /* _return */, const TUpdateStateRequest& /* params */) {
    return;
  }
  void Heartbeat(THeartbeatResponse& /* _return */, const THeartbeatRequest& /* params */) {
    return;
  }
};

typedef struct _StatestoreSubscriber_UpdateState_args__isset {
  _StatestoreSubscriber_UpdateState_args__isset() : params(false) {}
  bool params;
} _StatestoreSubscriber_UpdateState_args__isset;

class StatestoreSubscriber_UpdateState_args {
 public:

  StatestoreSubscriber_UpdateState_args() {
  }

  virtual ~StatestoreSubscriber_UpdateState_args() throw() {}

  TUpdateStateRequest params;

  _StatestoreSubscriber_UpdateState_args__isset __isset;

  void __set_params(const TUpdateStateRequest& val) {
    params = val;
    __isset.params = true;
  }

  bool operator == (const StatestoreSubscriber_UpdateState_args & rhs) const
  {
    if (!(params == rhs.params))
      return false;
    return true;
  }
  bool operator != (const StatestoreSubscriber_UpdateState_args &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const StatestoreSubscriber_UpdateState_args & ) const;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot);
  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

};


class StatestoreSubscriber_UpdateState_pargs {
 public:


  virtual ~StatestoreSubscriber_UpdateState_pargs() throw() {}

  const TUpdateStateRequest* params;

  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

};

typedef struct _StatestoreSubscriber_UpdateState_result__isset {
  _StatestoreSubscriber_UpdateState_result__isset() : success(false) {}
  bool success;
} _StatestoreSubscriber_UpdateState_result__isset;

class StatestoreSubscriber_UpdateState_result {
 public:

  StatestoreSubscriber_UpdateState_result() {
  }

  virtual ~StatestoreSubscriber_UpdateState_result() throw() {}

  TUpdateStateResponse success;

  _StatestoreSubscriber_UpdateState_result__isset __isset;

  void __set_success(const TUpdateStateResponse& val) {
    success = val;
    __isset.success = true;
  }

  bool operator == (const StatestoreSubscriber_UpdateState_result & rhs) const
  {
    if (!(success == rhs.success))
      return false;
    return true;
  }
  bool operator != (const StatestoreSubscriber_UpdateState_result &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const StatestoreSubscriber_UpdateState_result & ) const;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot);
  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

};

typedef struct _StatestoreSubscriber_UpdateState_presult__isset {
  _StatestoreSubscriber_UpdateState_presult__isset() : success(false) {}
  bool success;
} _StatestoreSubscriber_UpdateState_presult__isset;

class StatestoreSubscriber_UpdateState_presult {
 public:


  virtual ~StatestoreSubscriber_UpdateState_presult() throw() {}

  TUpdateStateResponse* success;

  _StatestoreSubscriber_UpdateState_presult__isset __isset;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot);

};

typedef struct _StatestoreSubscriber_Heartbeat_args__isset {
  _StatestoreSubscriber_Heartbeat_args__isset() : params(false) {}
  bool params;
} _StatestoreSubscriber_Heartbeat_args__isset;

class StatestoreSubscriber_Heartbeat_args {
 public:

  StatestoreSubscriber_Heartbeat_args() {
  }

  virtual ~StatestoreSubscriber_Heartbeat_args() throw() {}

  THeartbeatRequest params;

  _StatestoreSubscriber_Heartbeat_args__isset __isset;

  void __set_params(const THeartbeatRequest& val) {
    params = val;
    __isset.params = true;
  }

  bool operator == (const StatestoreSubscriber_Heartbeat_args & rhs) const
  {
    if (!(params == rhs.params))
      return false;
    return true;
  }
  bool operator != (const StatestoreSubscriber_Heartbeat_args &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const StatestoreSubscriber_Heartbeat_args & ) const;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot);
  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

};


class StatestoreSubscriber_Heartbeat_pargs {
 public:


  virtual ~StatestoreSubscriber_Heartbeat_pargs() throw() {}

  const THeartbeatRequest* params;

  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

};

typedef struct _StatestoreSubscriber_Heartbeat_result__isset {
  _StatestoreSubscriber_Heartbeat_result__isset() : success(false) {}
  bool success;
} _StatestoreSubscriber_Heartbeat_result__isset;

class StatestoreSubscriber_Heartbeat_result {
 public:

  StatestoreSubscriber_Heartbeat_result() {
  }

  virtual ~StatestoreSubscriber_Heartbeat_result() throw() {}

  THeartbeatResponse success;

  _StatestoreSubscriber_Heartbeat_result__isset __isset;

  void __set_success(const THeartbeatResponse& val) {
    success = val;
    __isset.success = true;
  }

  bool operator == (const StatestoreSubscriber_Heartbeat_result & rhs) const
  {
    if (!(success == rhs.success))
      return false;
    return true;
  }
  bool operator != (const StatestoreSubscriber_Heartbeat_result &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const StatestoreSubscriber_Heartbeat_result & ) const;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot);
  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

};

typedef struct _StatestoreSubscriber_Heartbeat_presult__isset {
  _StatestoreSubscriber_Heartbeat_presult__isset() : success(false) {}
  bool success;
} _StatestoreSubscriber_Heartbeat_presult__isset;

class StatestoreSubscriber_Heartbeat_presult {
 public:


  virtual ~StatestoreSubscriber_Heartbeat_presult() throw() {}

  THeartbeatResponse* success;

  _StatestoreSubscriber_Heartbeat_presult__isset __isset;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot);

};

class StatestoreSubscriberClient : virtual public StatestoreSubscriberIf {
 public:
  StatestoreSubscriberClient(boost::shared_ptr< ::apache::thrift::protocol::TProtocol> prot) :
    piprot_(prot),
    poprot_(prot) {
    iprot_ = prot.get();
    oprot_ = prot.get();
  }
  StatestoreSubscriberClient(boost::shared_ptr< ::apache::thrift::protocol::TProtocol> iprot, boost::shared_ptr< ::apache::thrift::protocol::TProtocol> oprot) :
    piprot_(iprot),
    poprot_(oprot) {
    iprot_ = iprot.get();
    oprot_ = oprot.get();
  }
  boost::shared_ptr< ::apache::thrift::protocol::TProtocol> getInputProtocol() {
    return piprot_;
  }
  boost::shared_ptr< ::apache::thrift::protocol::TProtocol> getOutputProtocol() {
    return poprot_;
  }
  void UpdateState(TUpdateStateResponse& _return, const TUpdateStateRequest& params);
  void send_UpdateState(const TUpdateStateRequest& params);
  void recv_UpdateState(TUpdateStateResponse& _return);
  void Heartbeat(THeartbeatResponse& _return, const THeartbeatRequest& params);
  void send_Heartbeat(const THeartbeatRequest& params);
  void recv_Heartbeat(THeartbeatResponse& _return);
 protected:
  boost::shared_ptr< ::apache::thrift::protocol::TProtocol> piprot_;
  boost::shared_ptr< ::apache::thrift::protocol::TProtocol> poprot_;
  ::apache::thrift::protocol::TProtocol* iprot_;
  ::apache::thrift::protocol::TProtocol* oprot_;
};

class StatestoreSubscriberProcessor : public ::apache::thrift::TDispatchProcessor {
 protected:
  boost::shared_ptr<StatestoreSubscriberIf> iface_;
  virtual bool dispatchCall(::apache::thrift::protocol::TProtocol* iprot, ::apache::thrift::protocol::TProtocol* oprot, const std::string& fname, int32_t seqid, void* callContext);
 private:
  typedef  void (StatestoreSubscriberProcessor::*ProcessFunction)(int32_t, ::apache::thrift::protocol::TProtocol*, ::apache::thrift::protocol::TProtocol*, void*);
  typedef std::map<std::string, ProcessFunction> ProcessMap;
  ProcessMap processMap_;
  //std::map<std::string, ProcessFunction> processMap_;
  void process_UpdateState(int32_t seqid, ::apache::thrift::protocol::TProtocol* iprot, ::apache::thrift::protocol::TProtocol* oprot, void* callContext);
  void process_Heartbeat(int32_t seqid, ::apache::thrift::protocol::TProtocol* iprot, ::apache::thrift::protocol::TProtocol* oprot, void* callContext);
 public:
  StatestoreSubscriberProcessor(boost::shared_ptr<StatestoreSubscriberIf> iface) :
    iface_(iface) {
    processMap_["UpdateState"] = &StatestoreSubscriberProcessor::process_UpdateState;
    processMap_["Heartbeat"] = &StatestoreSubscriberProcessor::process_Heartbeat;
  }

  virtual ~StatestoreSubscriberProcessor() {}
};

class StatestoreSubscriberProcessorFactory : public ::apache::thrift::TProcessorFactory {
 public:
  StatestoreSubscriberProcessorFactory(const ::boost::shared_ptr< StatestoreSubscriberIfFactory >& handlerFactory) :
      handlerFactory_(handlerFactory) {}

  ::boost::shared_ptr< ::apache::thrift::TProcessor > getProcessor(const ::apache::thrift::TConnectionInfo& connInfo);

 protected:
  ::boost::shared_ptr< StatestoreSubscriberIfFactory > handlerFactory_;
};

class StatestoreSubscriberMultiface : virtual public StatestoreSubscriberIf {
 public:
  StatestoreSubscriberMultiface(std::vector<boost::shared_ptr<StatestoreSubscriberIf> >& ifaces) : ifaces_(ifaces) {
  }
  virtual ~StatestoreSubscriberMultiface() {}
 protected:
  std::vector<boost::shared_ptr<StatestoreSubscriberIf> > ifaces_;
  StatestoreSubscriberMultiface() {}
  void add(boost::shared_ptr<StatestoreSubscriberIf> iface) {
    ifaces_.push_back(iface);
  }
 public:
  void UpdateState(TUpdateStateResponse& _return, const TUpdateStateRequest& params) {
    size_t sz = ifaces_.size();
    size_t i = 0;
    for (; i < (sz - 1); ++i) {
      ifaces_[i]->UpdateState(_return, params);
    }
    ifaces_[i]->UpdateState(_return, params);
    return;
  }

  void Heartbeat(THeartbeatResponse& _return, const THeartbeatRequest& params) {
    size_t sz = ifaces_.size();
    size_t i = 0;
    for (; i < (sz - 1); ++i) {
      ifaces_[i]->Heartbeat(_return, params);
    }
    ifaces_[i]->Heartbeat(_return, params);
    return;
  }

};

} // namespace

#endif
