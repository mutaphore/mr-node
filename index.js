"use strict";

const Master = require("./master/master").Master;
const Worker = require("./worker/worker").Worker;

const argv = require('yargs')
  .usage('mr-node [options] <master|worker> master_host:port [worker_host:port]')
  .example('mr-node worker localhost:5050 localhost:5051')
  .example('mr-node master localhost:5050')
  .number('m')
  .number('n')
  .default('m', 5)
  .default('n', 3)
  .alias('m', 'nmap')
  .alias('n', 'nreduce')
  .describe('m', 'number of map jobs')
  .describe('n', 'number of reduce jobs')
  .help('h')
  .argv

function validateArgs(callback) {
  const serviceType = argv._[0].toLowerCase();
  const masterAddr = argv._[1];
  const workerAddr = argv._[2];
  if (serviceType !== 'master' && serviceType !== 'worker') {
    return callback(new Error("First argument must be 'master' or 'worker'"));
  }
  if (serviceType === 'master') { 
    if (!argv.m || !argv.n) {
      return callback(new Error("Master service must provide valid m and n options"));
    }
    if (!masterAddr) {
      return callback(new Error("Missing master host address"));
    }
  } else if (serviceType === 'worker') {
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
    const service = (args.serviceType === 'master') ? 
      new Master(args.masterAddr, args.numMapJobs, args.numReduceJobs) :
      new Worker(args.workerAddr, args.masterAddr);
    service.start();
  });
}

main();