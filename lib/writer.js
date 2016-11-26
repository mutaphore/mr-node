"use strict";

const fs = require("fs");

class Writer {
  /**
   * Construct Writer
   * @param  {Object} config               - writer configuration object
   * @param  {String} config.targetType    - type of target the writer will write to
   * @param  {Object} config.targetOptions - target specific options: {path}
   */
  constructor(config) {
    if (!config.targetType || !config.targetOptions) {
      throw new Error("Missing targetType and/or targetOptions fields");
    }
    this.targetType = config.targetType;
    this.options = config.targetOptions;
    switch (config.targetType) {
      case 'fs':
        // check source options fields
        if (!this.options.path) {
          throw new Error("Missing required config options");
        }
        break;
      case 'hdfs':
        // check source options fields
        if (!(this.options.host && this.options.port && this.options.user && this.options.path)) {
          throw new Error("Missing required config options");
        }
        break;
      default:
        throw new Error("Unsupported target type " + this.targetType);
    }
  }
  /**
   * Get a writable stream for target
   * @return {Object} writable stream
   */
  createWriteStream() {
    switch (this.targetType) {
      case 'fs':
        const writeStream = fs.createWriteStream(this.options.path);
        writeStream.setDefaultEncoding('utf8');
        return writeStream;
      case 'hdfs':
        console.log("Not supported");
        return null;
      default:
        throw new Error("Unsupported target type " + this.targetType);
    }
  }
}

module.exports = Writer;