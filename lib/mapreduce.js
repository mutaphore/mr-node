'use strict';

const Transform = require('stream').Transform;

const STATE = Object.freeze({
  MAP: 1,
  REDUCE: 2,
  WAIT_MAP: 3,
  WAIT_RED: 4,
  MERGE: 5,
  END: 6
});

const OP = Object.freeze({
  MAP: 1,
  REDUCE: 2
});

// generate file name to be read by mapper
function mapFileName(fileName, mapJobNum) {
  return `mrtmp.${fileName}-${mapJobNum}`;
}

// generate file name to be read by reducer 
function reduceFileName(fileName, mapJobNum, reduceJobNum) {
  return `mrtmp.${fileName}-${mapJobNum}-${reduceJobNum}`;
}

// generate file name to be read in by merge
function mergeFileName(fileName, reduceJobNum) {
  return `mrtmp.${fileName}-res-${reduceJobNum}`;
}

// word count map function
function mapFunc(inputKey, inputValue) {
  // delete chars that are not alphanumeric
  const cleaned = inputValue.replace(/[^a-zA-z0-9]/gi, " ").trim();
  const kvList = [];
  if (!cleaned) {
    return kvList;
  }
  const words = cleaned.split(/\s+/gi);
  words.forEach((word) => {
    let kv = {};
    kv[word] = "1";
    kvList.push(kv);
  });
  return kvList;
}

// word count reduce function
function reduceFunc(interKey, interValues) {
  return interValues.length;
}

class MapStream extends Transform {
  constructor(options) {
    options.decodeStrings = false;
    options.objectMode = true;
    super(options);
    this.fileName = options.fileName;
    this.mapFunc = options.mapFunc;
  }

  _transform(chunk, encoding, callback) {
    return callback(null, this.mapFunc(this.fileName, chunk.line));
  }
}

module.exports = {
  STATE,
  OP,
  mapFileName,
  reduceFileName,
  mergeFileName,
  mapFunc,
  reduceFunc,
  MapStream,
};