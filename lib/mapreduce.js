'use strict';

const Transform = require('stream').Transform;
const Writable = require('stream').Writable;
const fs = require('fs');

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

// Stream that executes map function on input key/values
class MapStream extends Transform {
  /**
   * @param  {Object} options
   * @param  {String} options.fileName - input filename
   */
  constructor(options) {
    options.decodeStrings = false;
    options.objectMode = true;
    super(options);
    this.fileName = options.fileName;
  }

  _transform(chunk, encoding, callback) {
    return callback(null, mapFunc(this.fileName, chunk.line));
  }
}

// Stream that outputs intermediate files locally to approriate partitions
class InterFileWriteStream extends Writable {
  /**
   * @param  {Object} options
   * @param  {String} options.fileName - input filename
   * @param  {String} options.jobNum  - map job number
   * @param  {String} options.nReduce - number of reducers
   */
  constructor(options) {
    options.decodeStrings = false;
    options.objectMode = true;
    super(options);
    this.writeStreams = [];
    this.fileName = options.fileName;
    this.jobNum = options.jobNum;
    this.nReduce = options.nReduce;
    // create array of file output streams
    for (let i = 0; i < this.nReduce; i++) {
      this.writeStreams.push(fs.createWriteStream(reduceFileName(this.fileName, this.jobNum, i)));
    }
    // close writable streams on finish
    this.once('finish', this._cleanup)
  }

  // hashes a string and returns an integer
  _hashCode(s) {
    let hash = 0;
    let char;
    let i, l;
    if (s.length === 0) {
      return hash;
    }
    for (i = 0, l = s.length; i < l; i++) {
      char = s.charCodeAt(i);
      hash = ((hash << 5) - hash) + char;
      hash |= 0; // Convert to 32bit integer
    }
    return Math.abs(hash);
  }

  _write(chunk, encoding, callback) {
    chunk.forEach((kv) => {
      const reduceNum = this._hashCode(Object.keys(kv)[0]) % this.nReduce;
      this.writeStreams[reduceNum].write(JSON.stringify(kv) + '\n');
    });
    callback();
  }

  _cleanup() {
    this.writeStreams.forEach(writeStream => writeStream.end());
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
  InterFileWriteStream,
};