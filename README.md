# MapReduce in Node

A [MapReduce](https://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf) framework written in Nodejs that coordinates a master node and multiple distributed worker nodes to collectively run map and reduce jobs in parallel. Communication between nodes uses Google's RPC framework [gRPC](http://www.grpc.io/)  **This project is a work in progress**.

Currently this only supports WordCount (counting number of occurrences of each word) of the input textfile that is passed to master. I'm working on extending this be able to use any user-defined map or reduce functions.

## Getting started
Installing dependencies (only needed when running locally):
```
$ npm i
```
Make sure to copy the text file to be processed into the root directory. This will later be copied into the docker image for master.

### Docker
Building docker images `mr-master` and `mr-worker`:
```
$ docker-compose build
```

Running docker-compose to start the containers and begin running MapReduce:
```
$ docker-compose up
```

### Running locally
Alternatively, you could also run master locally:
```
$ node index.js master -f input.txt -m 2 -r 2 -a localhost:5050
```

and running a worker locally in a different process:
```
$ node index.js worker -m localhost:5050 -a localhost:6000
```


