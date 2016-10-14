"use strict";

const Master = require("./master/master").Master;
const Worker = require("./worker/worker").Worker;

const argv = require('yargs')
  .usage('$0 [options] <master|worker> master_host:port [worker_host:port]')
  .example('$0 -m  localhost:5050 -w localhost:5051 worker')
  .example('$0 -m localhost:5050 master')
  .number('m')
  .number('n')
  .default('m', 5)
  .default('n', 3)
  .alias('m', 'nmap')
  .alias('n', 'nreduce')
  .argv

function validateArgs(callback) {
  const serviceType = argv._[0];
  const masterAddr = argv._[1];
  const workerAddr = argv._[2];
  if (serviceType !== 'master' || serviceType !== 'worker') {
    return callback(new Error("Argument must be either 'master' or 'worker'"));
  }
  if (serviceType === 'master') { 
    if (!argv.m) {
      return callback(new Error("Master service must provide valid m options"));
    }
    if (!masterAddr) {
      return callback(new Error("Missing master host address"));
    }
  } else if (serviceType === 'worker') {
    if (!argv.m || !argv.n) {
      return callback(new Error("Worker service must provide valid m and n options"));
    }
    if (!masterAddr || !workerAddr) {
      return callback(new Error("Missing master and/or worker host address(es)"));
    }
  }
  const args = {
    serviceType: serviceType,
    masterAddr: masterAddr,
    workerAddr: workerAddr,
    numMapJobs: argv.m,
    numReduceJobs: argv.n
  }
  return callback(null, args);
}

function main() {
  validateArgs((err, args) => {
    if (err) {
      console.error(err);
      process.exit(-1);
    }
    let service;
    if (args.serviceType === 'master') {
      service = new Master(args.masterAddr, args.numMapJobs, args.numReduceJobs);
    } else {
      service = new Worker(args.workerAddr, args.masterAddr)
    }
    service.start();
  });
}

main();