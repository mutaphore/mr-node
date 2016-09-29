const grpc = require("grpc");

const PROTO_PATH = "../protos/master_rpc.proto";
const rpc = grpc.load(PROTO_PATH).masterrpc;

const master = new rpc.Master('localhost:50051', grpc.credentials.createInsecure());

function main() {
  master.ping({ host: "worker1" }, function (err, response) {
    if (err) {
      return console.error(err);
    }
    console.log(response);
  });
}

if (require.main === module) {
  main();
}
