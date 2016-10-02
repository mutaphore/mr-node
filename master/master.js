"use strict";

const grpc = require("grpc");

const PROTO_PATH = "./master_rpc.proto";
const rpc = grpc.load(PROTO_PATH).masterrpc;

function ping(request, callback) {
  const reply = { 
    host: "master"
  };
  return callback(null, reply);
}

function getServer() {
  const server = new grpc.Server();
  server.addProtoService(rpc.Master.service, {
    ping: ping
  });
  return server;
}

function main() {
  const server  = getServer();
  const port    = process.argv.length > 2 ? process.argv[2] : 50051;
  const address = '0.0.0.0:' + port;
  server.bind(address, grpc.ServerCredentials.createInsecure());
  server.start();
}

// If this is run as a script, start a server on an unused port
if (require.main === module) {
  main();
}

exports.getServer = getServer;
