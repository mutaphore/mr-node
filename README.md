# MapReduce in Node

[MapReduce](https://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf) framework written in Nodejs that coordinates a master node with multiple worker nodes to collectively runs map and reduce jobs in parallel. This project is a work in progress.

Currently this only supports WordCount of the input file to master. I'm working on extending it to use any user defined map or reduce functions.

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


