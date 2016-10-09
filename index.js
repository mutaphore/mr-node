"use strict";

const argv = require('yargs')
  .demand(1)
  .alias('m', 'masterAddress:port')
  .alias('w', 'workerAddress:port')
  .argv

console.log(argv._[0]);