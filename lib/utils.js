'use strict';

const fs = require('fs');
const async = require('async');
const Buffer = require('buffer').Buffer;

// splits a file as evenly as possible by lines into m parts
function splitFileByLines(filePath, m, callback) {
  const fileSize = fs.statSync(filePath).size;
  const avgSize = Math.floor(fileSize / m);
  const remainder = fileSize % m;
  let lineCount = 0;
  fs.open(filePath, 'r', (err, fd) => {
    if (err) {
      return callback(err);
    }
    const buffer = Buffer.alloc(100);
    const lineSizes = [];
    let numLineBytes = 0;
    async.doDuring(
      (callback) => {
        fs.read(fd, buffer, 0, 100, null, (err, bytesRead) => {
          for (let i = 0; i < bytesRead; i++) {
            if (buffer[i] === 10) {
              lineSizes.push(numLineBytes);
              numLineBytes = 0
            } else {
              numLineBytes++;
            }
          }
          callback(err, bytesRead);
        });
      },
      (bytesRead, callback) => {
        callback(null, bytesRead === 100);
      }, (err) => {
        if (err) {
          return callback(err);
        }
        return callback(null, lineSizes);
      }
    );
  });
}

module.exports = {
  splitFileByLines,
};