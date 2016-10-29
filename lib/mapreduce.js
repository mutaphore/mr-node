'use strict';

const STATE = {
  MAP     : 1,
  REDUCE  : 2,
  WAIT_MAP: 3,
  WAIT_RED: 4,
  MERGE   : 5,
  END     : 6
};
Object.freeze(STATE);

const OP = {
  MAP   : 1,
  REDUCE: 2
};
Object.freeze(OP);

module.exports = {
  STATE: STATE,
  OP: OP
}