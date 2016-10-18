"use strict";

const grpc    = require("grpc");
const config  = require("config");
const async   = require("async");
const rpcFunc = require("./masterrpc");

class Master {

  constructor(masterAddr, nMap, nReduce) {
    // general master configs
    this.masterAddr = masterAddr;
    this.nMap = nMap;
    this.nReduce = nReduce;
    this.numMapJobsDone = 0;
    this.numReduceJobsDone = 0;
    this.heartbeatInterval = 5000;

    // create master rpc server
    this.server = new grpc.Server();
    this.server.bind(masterAddr, grpc.ServerCredentials.createInsecure());

    // worker directory: map workerId -> workerInfo object
    this.workers = {};

    // worker queues
    this.workerQueue = [];

    // add rpc functions
    this.masterDescriptor = grpc.load(config.get("proto.master")).masterrpc;
    this.workerDescriptor = grpc.load(config.get("proto.worker")).workerrpc;
    this.server.addProtoService(this.masterDescriptor.Master.service, {
      ping    : rpcFunc.ping.bind(this),
      register: rpcFunc.register.bind(this),
      jobDone : rpcFunc.jobDone.bind(this)
    });
  }

  _sendJob(operation, jobNumber) {

  }

  _distributeJobs() {

  }

  start() {
    this.server.start();
  }
}

exports.Master = Master;
