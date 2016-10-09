"use strict";

const grpc = require("grpc");
const uuid = require("uuid");

const rpcFunc = require("./workerrpc");

const MASTER_PROTO_PATH = "../protos/master.proto";
const WORKER_PROTO_PATH = "../protos/worker.proto";

class Worker {
  /**
   * Create a Worker instance
   * @param  {String} workerAddr - worker address with format ipaddress:port
   * @param  {String} masterAddr - master address with format ipaddress:port
   */
  constructor(workerAddr, masterAddr) {
    this.workerId = uuid.v4();
    this.workerAddr = workerAddr;
    this.masterAddr = masterAddr;

    this.masterDescriptor = grpc.load(MASTER_PROTO_PATH).masterrpc;
    this.workerDescriptor = grpc.load(WORKER_PROTO_PATH).workerrpc;

    // load master rpc service
    this.master = new this.masterDescriptor.Master(masterAddr, grpc.credentials.createInsecure());

    // create worker rpc server
    this.server = new grpc.Server();
    this.server.bind(workerAddr, grpc.ServerCredentials.createInsecure());

    // add rpc functions
    this.server.addProtoService(this.workerDescriptor.Worker.service, {
      ping: rpcFunc.ping.bind(this)
    });
  }

  // ---- Worker private functions ----

  // register worker with master
  _register() {
    const data = {
      worker_id: this.workerId,
      worker_address: this.workerAddr 
    };
    this.master.register(data, (err, resp) => {
      if (err) {
        console.log("Failed to register with master");
        throw err;
      }
      console.log(`Connected with master: ${this.masterAddr}`);
    });
  }

  // ---- Worker public functions

  // run the worker
  start() {
    this.server.start();
    this._register();
  }
}

// To run worker: 
// node worker.js workerAddr:port masterAddr:port
if (require.main === module) {
  if (process.argv.length !== 4) {
    throw new Error("Invalid number of arguments");
  }
  const worker = new Worker(process.argv[2], process.argv[3]);
  worker.start();
}

exports.Worker = Worker;