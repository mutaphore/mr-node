'use strict';

module.exports.STATE = Object.freeze({
  MAP     : 1,
  REDUCE  : 2,
  WAIT_MAP: 3,
  WAIT_RED: 4,
  MERGE   : 5,
  END     : 6
});

module.exports.OP = Object.freeze({
  MAP   : 1,
  REDUCE: 2
});

// word count map function
module.exports.mapFunc = (inputKey, inputValue) => {
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
};

// word count reduce function
module.exports.reduceFunc = (interKey, interValues) => {
  return interValues.length;
};
