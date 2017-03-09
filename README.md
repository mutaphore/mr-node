# MapReduce in Node

[MapReduce](https://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf) framework written in Nodejs. Coordinates 1 master node with multiple worker nodes that collectively runs map and reduce jobs in parallel. This project is a work in progress.

Currently this only supports WordCount of the input file to master. I'm working on extending it to use any user defined map or reduce functions.

## Getting started
Installing dependencies (only needed when running locally):
```
$ npm i
```
Make sure to copy your favorite text file to the root directory. This will later be copied into the docker image for master.

### Docker
Building docker images:
```
$ docker-compose build
```

Running docker-compose to start the containers:
```
$ docker-compose up
```

### Running locally
Alternatively, you could also run master locally:
```
$ node index.js master -f myfile.txt -m 3 -n 3 localhost:5000
```

and running a worker locally in a different process:
```
$ node index.js worker localhost:5000 localhost:6000
```


