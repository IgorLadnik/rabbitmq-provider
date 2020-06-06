const _ = require('lodash');

module.exports.delayMs = (duration) =>
    new Promise(resolve =>
        setTimeout(() => {
            resolve();
        }, duration)
    );

module.exports.isEmpty = (str) =>
    _.isNil(str) || str.length === 0;

module.exports.flatten = function flatten(arr, result = []) {
    for (let i = 0, length = arr.length; i < length; i++) {
        const value = arr[i];
        if (Array.isArray(value))
            flatten(value, result);
        else
            result.push(value);
    }

    return result;
};
