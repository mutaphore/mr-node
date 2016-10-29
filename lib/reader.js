"use strict";

class Reader {
  /**
   * Construct reader
   * @param  {Object} config               - reader configuration object
   * @param  {String} config.sourceType    - type of source the reader will read from
   * @param  {Object} config.sourceOptions - source-specific options
   */
  constructor(config) {
    this.sourceType = config.sourceType;
    this.options = config.sourceOptions;
    
    switch (config.sourceType) {
      case 'fs':
        // check source options fields
        if (!this.options.path) {
          throw new Error("Missing required config options");
        }
        this.client = require("fs");
        break;
      case 'hdfs':
        // TODO: check source options fields
        this.client = require("webhdfs").createClient({
          host: this.options.host,
          port: this.options.port,
          user: this.options.user,
          path: this.options.path
        });
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
        return  this.client.createReadStream(this.options.path, {
          flags: 'r',
          encoding: 'utf8'
        });
      case 'hdfs':
        return this.client.createReadStream(this.options.path);
      default:
        throw new Error("Unsupported resource type " + source);
    }
  }
}

module.exports = {
  Reader: Reader
};