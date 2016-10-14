"use strict";

const grpc    = require("grpc");
const async   = require("async");
const rpcFunc = require("./masterrpc");

const MASTER_PROTO_PATH = "../protos/master.proto";
const WORKER_PROTO_PATH = "../protos/worker.proto";

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
    this.masterDescriptor = grpc.load(MASTER_PROTO_PATH).masterrpc;
    this.workerDescriptor = grpc.load(WORKER_PROTO_PATH).workerrpc;
    this.server.addProtoService(this.masterDescriptor.Master.service, {
      ping    : rpcFunc.ping.bind(this),
      register: rpcFunc.register.bind(this)
    });
  }

  _sendJob(operation, jobNumber) {
    this.wor
  }

  _distributeJob() {

  }

  start() {
    this.server.start();
  }
}

// To run master:
// node master masterAddr:port
if (require.main === module) {
  if (process.argv.length !== 3) {
    throw new Error("Invalid number of arguments");
  }
  const master = new Master(process.argv[2], 1, 1);
  master.start();
}

exports.Master = Master;
