"use strict";

const grpc = require("grpc");

const PROTO_PATH = "./worker_rpc.proto";

class Worker {
  /**
   * Create a Worker instance
   * @param  {String} workerAddr - worker address with format ipaddress:port
   * @param  {String} masterAddr - master address with format ipaddress:port
   */
  constructor(workerAddr, masterAddr) {
    this.workerAddr = workerAddr;
    this.masterAddr = masterAddr;

    const rpc   = grpc.load(PROTO_PATH).masterrpc;
    this.master = new rpc.Master(masterAddr, grpc.credentials.createInsecure());

    this.server = new grpc.Server();
    this.server.bind(workerAddr, grpc.ServerCredentials.createInsecure());
    this.server.addProtoService(rpc.Worker.service, {
      ping: _rpc_ping
    });

    this._start();
  }

  // ---- Worker RPC functions ----

  // get worker host info
  _rpc_ping(request, callback) {
    const reply = { 
      host: this.workerAddr
    };
    return callback(null, reply);
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
}

exports.Worker = Worker;