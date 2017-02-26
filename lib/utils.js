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
              lineSizes.push(numLineBytes+1);
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
        // splits are byte boundaries inclusive [start, end] with start counting at 0
        const splits = [];
        let curSize = 0;
        let startByte = 0;
        for (let i = 0; i < lineSizes.length; i++) {
          if (curSize < avgSize) {
            curSize += lineSizes[i];
          } else {
            splits.push({
              start: startByte,
              end: startByte + curSize - 1,
            });
            startByte = startByte + curSize;
            curSize = lineSizes[i];
          }
        }
        // add final split
        splits.push({
          start: startByte,
          end: fileSize,
        });
        if (splits.length !== m) {
          // TODO: force split to m parts
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