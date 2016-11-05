"use strict";

const readline = require("readline");

class Reader {
  /**
   * Construct reader
   * @param  {Object} config               - reader configuration object
   * @param  {String} config.sourceType    - type of source the reader will read from
   * @param  {Object} config.sourceOptions - source-specific options
   */
  constructor(config) {
    if (!config.sourceType || !config.sourceOptions) {
      throw new Error("Missing sourceType and/or sourceOptions fields");
    }
    this.sourceType = config.sourceType;
    this.options = config.sourceOptions;
    switch (config.sourceType) {
      case 'fs':
        // check source options fields
        if (!this.options.path) {
          throw new Error("Missing required config options");
        }
        break;
      case 'hdfs':
        // TODO: check source options fields
        if (!(this.options.host && this.options.port && this.options.user && this.options.path)) {
          throw new Error("Missing required config options");
        }
        break;
      default:
        throw new Error("Unsupported resource type " + source);
    }
  }
  /**
   * Get a readable stream from source
   * @return {Object} readable stream
   */
  createReadStream() {
    switch (this.sourceType) {
      case 'fs':
        const fs = require("fs");
        return readline.createInterface({ input: fs.createReadStream(this.options.path) });
      case 'hdfs':
        const client = require("webhdfs").createClient({
          host: this.options.host,
          port: this.options.port,
          user: this.options.user,
          path: this.options.path
        });
        return readline.createInterface({ input: client.createReadStream(this.options.path) });
      default:
        throw new Error("Unsupported resource type " + source);
    }
  }
}

module.exports = {
  Reader: Reader
};