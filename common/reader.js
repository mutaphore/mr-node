"use strict";

class Reader {
  /**
   * Config for reader
   * @param  {Object} config
   * @param  {String} config.sourceType
   * @param  {Object} config.sourceOptions
   * @param  {String} config.sourceOptions.host
   * @param  {String} config.sourceOptions.port
   * @param  {String} config.sourceOptions.user
   * @param  {String} config.sourceOptions.path
   */
  constructor(config) {
    switch (config.sourceType) {
      case 'hdfs':
        // TODO: check source options
        this.client = require("webhdfs").createClient({
          host: config.sourceOptions.host,
          port: config.sourceOptions.port,
          user: config.sourceOptions.user,
          path: config.sourceOptions.path
        });
        break;
      default:
        throw new Error("Unsupported resource type " + source);
        break;
    }
  }
}

class MapperReader extends Reader {
  constructor(config, mapJob) {
    super(config);
    this.mapJob = mapJob;
  }

}