'use strict';

const STATE = Object.freeze({
  MAP     : 1,
  REDUCE  : 2,
  WAIT_MAP: 3,
  WAIT_RED: 4,
  MERGE   : 5,
  END     : 6
});

const OP = Object.freeze({
  MAP   : 1,
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

// generate file name to be read in by merger
function mergeFileName(fileName, reduceJobNum) {
  return `mrtmp.${fileName}-res-${reduceJobNum}`;
}

// word count map function
function mapFunc(inputKey, inputValue) {
  // delete chars that are not alphanumeric or spaces
  const cleaned = inputValue.replace(/[^a-zA-z0-9\s]/gi, "");
  const words = cleaned.split(/\s+/gi);
  const kvList = [];
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

module.exports = {
  STATE: STATE,
  OP: OP,
  mapFileName: mapFileName,
  reduceFileName: reduceFileName,
  mapFunc: mapFunc,
  reduceFunc: reduceFunc
};