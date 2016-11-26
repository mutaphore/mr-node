"use strict";

const Master = require("./master/master").Master;
const Worker = require("./worker/worker").Worker;

const argv = require('yargs')
  .usage('mr-node [options] <master|worker> master_host:port [worker_host:port]')
  .example('mr-node worker localhost:5050 localhost:5051')
  .example('mr-node master localhost:5050')
  .number('m')
  .number('n')
  .string('f')
  .default('m', 1)
  .default('n', 1)
  .alias('m', 'nmap')
  .alias('n', 'nreduce')
  .alias('f', 'infile')
  .describe('m', 'number of map jobs')
  .describe('n', 'number of reduce jobs')
  .describe('f', 'input filename')
  .help('h')
  .argv

function validateArgs(callback) {
  const serviceType = argv._[0].toLowerCase();
  const masterAddr = argv._[1];
  const workerAddr = argv._[2];
  if (serviceType !== 'master' && serviceType !== 'worker') {
    return callback(new Error("First argument must be 'master' or 'worker'"));
  }
  if (!argv.m || !argv.n) {
    return callback(new Error("Must provide valid m and n options"));
  }
  if (serviceType === 'master') {
    if (!masterAddr) {
      return callback(new Error("Missing master host address"));
    }
    if (!argv.f) {
      return callback(new Error("Must provide input filename"));
    }
  } else if (serviceType === 'worker') {
    if (!masterAddr || !workerAddr) {
      return callback(new Error("Missing master and/or worker host address(es)"));
    }
  }
  const validatedArgs = {
    serviceType: serviceType,
    masterAddr: masterAddr,
    workerAddr: workerAddr,
    numMapJobs: argv.m,
    numReduceJobs: argv.n,
    fileName: argv.f
  }
  return callback(null, validatedArgs);
}

function main() {
  validateArgs((err, args) => {
    if (err) {
      console.error(err);
      process.exit(1);
    }
    const service = (args.serviceType === 'master') ? 
      new Master(args.masterAddr, args.numMapJobs, args.numReduceJobs, args.fileName) :
      new Worker(args.workerAddr, args.masterAddr, args.numMapJobs, args.numReduceJobs);
    service.start();
  });
}

main();