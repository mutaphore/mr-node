"use strict";

const grpc   = require("grpc");
const config = require("config");
const uuid   = require("uuid");
const async  = require("async");

const rpcFunc = require("./workerrpc");
const mr      = require("../lib/mapreduce");

const MASTER_PROTO_PATH = "./protos/master.proto";
const WORKER_PROTO_PATH = "./protos/worker.proto";

class Worker {
  /**
   * Create a Worker instance
   * @param  {String} workerAddr - worker address with format ipaddress:port
   * @param  {String} masterAddr - master address with format ipaddress:port
   */
  constructor(workerAddr, masterAddr, nMap, nReduce) {
    this.workerId   = uuid.v4();
    this.workerAddr = workerAddr;
    this.masterAddr = masterAddr;
    this.nMap       = nMap;
    this.nReduce    = nReduce;

    this.masterDescriptor = grpc.load(config.get("proto.master")).masterrpc;
    this.workerDescriptor = grpc.load(config.get("proto.worker")).workerrpc;

    // load master rpc service
    this.master = new this.masterDescriptor.Master(masterAddr, grpc.credentials.createInsecure());

    // create worker rpc server
    this.server = new grpc.Server();
    this.server.bind(workerAddr, grpc.ServerCredentials.createInsecure());

    // add rpc functions
    this.server.addProtoService(this.workerDescriptor.Worker.service, {
      ping: rpcFunc.ping.bind(this),
      doJob: rpcFunc.doJob.bind(this)
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
        process.exit(-1);
      }
      if (!resp) {
        console.log("No response received from master");
        process.exit(-1);
      }
      console.log(`Connected with master: ${this.masterAddr}`);
    });
  }

  _doMap(jobNum, fileName) {
    const options = {
      sourceType: 'fs',
      sourceOptions: {
        path: `mrtmp.${fileName}-${jobNum}`
      } 
    };
    const r = new reader.Reader(options);
    const readable = r.createReadStream();
    readable.on('line', (line) => {
      mr.mapFunc(fileName, line);
    });
    readable.on('close', () => {
      // signal to master this map job is done
      const data = {
        worker_id: this.workerId,
        job_num: jobNum,
        operation: mr.OP.MAP,
        error: ""
      };
      this.master.jobDone(data);
    });
  }

  _doReduce(jobNum, fileName) {
    // iterate through all map jobs for this reducer
    const tasks = [];
    for (let mapJobNum = 0; mapJobNum < this.nMap; mapJobNum++) {
      tasks.push((callback) => {
        const options = {
          sourceType: 'fs',
          sourceOptions: {
            path: `mrtmp.${fileName}-${mapJobNum}-${jobNum}`
          }
        };
        const r = new reader.Reader(options);
        const readable = r.createReadStream();
        readable.on('line', (line) => {
          const parts = line.split(",");
          mr.reduceFunc(parts.shift(), parts);
        });
        readable.on('close', callback);
      });
    }
    async.parallel(tasks, (err, results) => {
      // signal to master this reduce job is done
      const data = {
        worker_id: this.workerId,
        job_num: jobNum,
        operation: mr.OP.REDUCE,
        error: err ? err.message : ""
      };
      this.master.jobDone(data);
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