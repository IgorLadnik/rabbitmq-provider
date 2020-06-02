const _ = require('lodash');

module.exports.Logger = class Logger {
    constructor(fnLog) {
        this.fnLog = fnLog;
    }

    log = (msg) => {
        if (!_.isNil(this.fnLog))
            this.fnLog(msg);
    }
}

module.exports.delayMs = (duration) =>
    new Promise(resolve =>
        setTimeout(() => {
            resolve();
        }, duration)
    );

module.exports.isEmpty = (str) =>
    _.isNil(str) || str.length === 0;



