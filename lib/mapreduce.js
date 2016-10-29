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