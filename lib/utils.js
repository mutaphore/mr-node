'use strict';

const fs = require('fs');
const async = require('async');
const Buffer = require('buffer').Buffer;

const BUF_SIZE = 100;

// algorithm to split a file as evenly as possible by lines into m splits
function splitFileByLines(filePath, m, callback) {
  fs.open(filePath, 'r', (err, fd) => {
    if (err) {
      return callback(err);
    }
    const buffer = Buffer.alloc(BUF_SIZE);
    const lineSizes = [];
    let numLineBytes = 0;
    return async.doDuring(
      (callback) => {
        fs.read(fd, buffer, 0, BUF_SIZE, null, (err, bytesRead) => {
          if (err) {
            return callback(err);
          }
          for (let i = 0; i < bytesRead; i++) {
            // check for the new line character
            if (buffer[i] === 0xA) {
              lineSizes.push(numLineBytes);
              numLineBytes = 0
            } else {
              numLineBytes++;
            }
          }
          return callback(null, bytesRead);
        });
      },
      (bytesRead, callback) => {
        callback(null, bytesRead === BUF_SIZE);
      }, (err) => {
        if (err) {
          return callback(err);
        }
        const fileSize = fs.statSync(filePath).size;
        const avgSize = fileSize / m;
        // splits end where the "end" is inclusive, i.e. (start, end]
        const splits = [];
        const splitSizes = [];
        let curSize = 0;
        for (let i = 0; i < lineSizes.length; i++) {
          if (curSize < avgSize) {
            curSize += lineSizes[i];
          } else {
            splits.push(i);
            splitSizes.push(curSize);
            curSize = lineSizes[i];
          }
        }
        splitSizes.push(curSize);
        if (splits.length !== m-1) {
          // TODO: must split to m parts
          return callback(new Error(`Failed to split file into ${m} parts`));
        }
        return callback(null, splits);
      }
    );
  });
}

module.exports = {
  splitFileByLines,
};