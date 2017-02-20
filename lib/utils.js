'use strict';

const fs = require('fs');

function countFileLines(filePath, callback) {
  const rs = fs.createReadStream(filePath);
  let lineCount = 0;

  rs.on('data', (chunk) => {
    for (let b of chunk.toString('utf8')) {
      if (b === '\n') {
        lineCount++;
      }
    }
  });

  rs.on('error', (err) => {
    callback(err);
  });

  rs.on('close', () => {
    callback(null, lineCount);
  });
}

module.exports = {
  countFileLines,
};