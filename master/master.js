"use strict";

const grpc    = require("grpc");
const rpcFunc = require("./masterrpc");

const WORKER_PROTO_PATH = "../protos/worker.proto";

class Master {
  constructor(masterAddr) {
    this.masterAddr = masterAddr;

    // create master rpc server
    this.server = new grpc.Server();
    this.server.bind(masterAddr, grpc.ServerCredentials.createInsecure());

    // add rpc functions
    this.server.addProtoService({
      ping    : rpcFunc.ping.bind(this),
      register: rpcFunc.register.bind(this)
    });
  }

  start() {
    this.server.start();
  }
}

// If this is run as a script, start a server on an unused port
if (require.main === module) {
  const master = new Master(process.argv[2]);
  master.start();
}

exports.Master = Master;
