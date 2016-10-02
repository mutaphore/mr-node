"use strict";

const grpc = require("grpc");

const PROTO_PATH = "./worker_rpc.proto";
const rpc = grpc.load(PROTO_PATH).workerrpc;

const master = new rpc.Master('localhost:50051', grpc.credentials.createInsecure());

class Worker {
  /**
   * Create a Worker instance
   * @param  {String} me     - worker address with format ipaddress:port
   * @param  {String} master - master address with format ipaddress:port
   */
  constructor(me, master) {
    this.host   = me;
    this.master = master;
    this.server = new grpc.Server();
    this.server.bind(this.host, grpc.ServerCredentials.createInsecure());
    this.server.addProtoService(rpc.Worker.service, {
      ping: ping
    });
    this._start();
  }

  // ---- Worker RPC functions ----

  // ping worker to get host info
  ping(request, callback) {
    const reply = { 
      host: this.host
    };
    return callback(null, reply);
  }

  // ---- Worker private functions ----

  // register worker with master
  _register() {

  }

  // ---- Worker public functions

  // run the worker
  start() {
    this.server.start();
    this._register();
    master.ping({ host: "worker1" }, function (err, response) {
      if (err) {
        return console.error(err);
      }
      console.log(response);
    });
  }
}

if (require.main === module) {
  // To run worker: 
  // node worker.js workerAddr:port masterAddr:port
  const worker = new Worker(process.argv[2], process.argv[3]);
}

exports.getServer = getServer;