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
    switch (config.sourceType) {
      case 'hdfs':
        // TODO: check source options fields
        this.client = require("webhdfs").createClient({
          host: config.sourceOptions.host,
          port: config.sourceOptions.port,
          user: config.sourceOptions.user,
          path: '/webhdfs/v1'
        });
        this.filePath = config.sourceOptions.path
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
      case 'hdfs':
        return this.client.createReadStream(this.filePath)
      default:
        throw new Error("Unsupported resource type " + source);
    }
  }
}

class MapperReader extends Reader {
  /**
   * Construct MapperReader
   * @param  {Object}  config - reader configuration object
   * @param  {Integer} mapJob - map job number
   */
  constructor(config, mapJob) {
    super(config);
    this.mapJob = mapJob;  // map job number
  }
}