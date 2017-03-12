#!/usr/bin/env node

"use strict";

const Master = require("./master/master").Master;
const Worker = require("./worker/worker").Worker;

const argv = require('yargs')
  .usage(`Usage: $0 <command> [options]`)
  .example('master', 'node index.js master -f input.txt -a localhost:5050')
  .example('worker', 'node index.js worker -m localhost:5050 -a localhost:5051')
  .command('master', 'run MapReduce master service', {
    address: {
      alias: 'a',
      description: 'master host ipaddress:port',
      default: 'localhost:5000',
      type: 'string',
    },
    num_map: {
      alias: 'm',
      description: 'number of mappers',
      default: 1,
      type: 'number',
    },
    num_red: {
      alias: 'r',
      description: 'number of reducers',
      default: 1,
      type: 'number',
    },
    file: {
      alias: 'f',
      description: 'input file name',
      demandOption: '!Provide an input file name',
    },
  })
  .command('worker', 'run MapReduce worker service', {
    address: {
      alias: 'a',
      description: 'worker host ipaddress:port',
      default: 'localhost:6000',
      type: 'string',
    },
    master: {
      alias: 'm',
      description: 'master host ipaddress:port',
      demandOption: '!Provide master node address',
      type: 'string',
    }
  })
  .demandCommand(1, 1, 'Enter a command')
  .recommendCommands()
  .epilogue('for more information, check out https://github.com/mutaphore/mr-node')
  .help('h')
  .argv;

function main() {
  const command = argv._[0];
  let service;
  if (command === 'master') {
    service = new Master(argv.address, argv.num_map, argv.num_red, argv.file);
  } else if (command === 'worker') {
    service = new Worker(argv.address, argv.master);
  } else {
    console.error(`Invalid command "${command}"`);
    process.exit(1);
  }
  service.start((err) => {
    if (err) {
      console.error(err);
      process.exit(1);
    }
  });
}

main();