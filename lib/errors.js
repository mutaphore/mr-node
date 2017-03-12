'use strict';

function invalidMasterState() { return new Error('Invalid Master state'); }

function invalidResponse() { return new Error('Invalid response received'); }

function noResponseFromMaster() { return new Error('No response received from master'); }

function notOkResponse() { return new Error('Response received was not "ok"'); }

function invalidValues() { return new Error('Invalid values received'); }

module.exports = {
  invalidMasterState,
  invalidResponse,
  noResponseFromMaster,
  notOkResponse,
  invalidValues,
};
