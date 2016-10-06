"use strict";

const grpc    = require("grpc");
const rpcFunc = require("./workerrpc");

const WORKER_PROTO_PATH = "../protos/worker.proto";

class Worker {
  /**
   * Create a Worker instance
   * @param  {String} workerAddr - worker address with format ipaddress:port
   * @param  {String} masterAddr - master address with format ipaddress:port
   */
  constructor(workerAddr, masterAddr) {
    this.workerAddr = workerAddr;
    this.masterAddr = masterAddr;

    // load master rpc service
    const masterrpc   = grpc.load(WORKER_PROTO_PATH).masterrpc;
    this.master = new masterrpc.Master(masterAddr, grpc.credentials.createInsecure());

    // create worker rpc server
    this.server = new grpc.Server();
    this.server.bind(workerAddr, grpc.ServerCredentials.createInsecure());

    // add rpc functions
    this.server.addProtoService(rpc.Worker.service, {
      ping: rpcFunc.ping.bind(this)
    });
  }

  // ---- Worker private functions ----

  // register worker with master
  _register(master) {
    this.master.register();
  }

  // ---- Worker public functions

  // run the worker
  start() {
    this.server.start();
    this._register(this.master);
    master.ping({ host: this.workerAddr }, (err, response) => {
      if (err) {
        return console.error(err);
      }
      console.log(response);
    });
  }
}

// To run worker: 
// node worker.js workerAddr:port masterAddr:port
if (require.main === module) {
  const worker = new Worker(process.argv[2], process.argv[3]);
  worker.start();
}

exports.Worker = Worker;