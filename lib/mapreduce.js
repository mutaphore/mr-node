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

module.exports.mapFunc = (inputKey, inputValue) => {
  // TODO: word count map function
};

module.exports.reduceFunc = (interKey, interValues) => {
  // TODO: word count reduce function
};