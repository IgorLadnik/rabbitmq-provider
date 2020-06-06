const _ = require('lodash');

module.exports.Logger = class Logger {
    constructor(externalLogger) {
        this.externalLogger = externalLogger;
    }

    log = (msg) => {
        setImmediate(() => {
        if (_.isNil(this.externalLogger))
            console.log(msg)
        else
            this.externalLogger.log(msg);
        });
    }
}
