"use strict";

class Writer {
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
        throw new Error("Unsupported target type " + source);
    }
  }

  createWriteStream() {

  }
}

module.exports = Writer;