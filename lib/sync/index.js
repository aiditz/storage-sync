'use strict'

var Exceptions = require('../exceptions'),
    ArgumentException = Exceptions.ArgumentException,
    NotFoundException = Exceptions.NotFoundException,
    Exception = Exceptions.Exception,
    Container = require('../container'),
    async = require('async'),
    EventEmitter = require('events'),
    path = require('path'),
    _ = require('lodash');

class StorageSync extends EventEmitter {
    constructor(sourceContainer, destinationContainer, options) {
        super();
        if (!sourceContainer) {
            throw new ArgumentException('Argument sourceContainer is required');
        }
        if (!destinationContainer) {
            throw new ArgumentException('Argument destinationContainer is required');
        }
        this._sourceContainer = sourceContainer;
        this._sourceFiles = options.sourceFiles;
        this._destinationContainer = destinationContainer;
        this._destinationFiles = options.destinationFiles;
        this.used = false;

        if (!options) {
            options = {};
        }

        if (typeof options.stopOnError !== 'boolean') {
          options.stopOnError = false;
        }

        if (!Array.isArray(options.skipFileNames)) {
          options.skipFileNames = [];
        }

        this.options = options;
    }

    sync() {
        if (this.used) {
            throw new Exception('Sync can be invoked only once');
        }

        this.used = true;
        const self = this;

        return new Promise((resolve, reject) => {
            async.waterfall([
                (cb) => {
                    if (self._sourceFiles) {
                        return cb(null);
                    }

                    self.emit('countingStarted');
                    self._sourceContainer
                        .listFiles()
                        .then((sourceFiles) => {
                            self.emit('countingDone', sourceFiles.length);
                            self._sourceFiles = sourceFiles;
                            cb(null);
                        })
                        .catch(cb);
                },
                (sourceFiles, cb) => {
                    if (self._destinationFiles) {
                        return cb(null);
                    }

                    self.emit('countingStarted');
                    self._destinationContainer
                        .listFiles()
                        .then((destinationFiles) => {
                            self.emit('countingDone', destinationFiles.length);
                            self._destinationFiles = destinationFiles;
                            cb(null);
                        })
                        .catch(cb);
                },
                (sourceFiles, destinationFiles, cb) => {
                    const actions = [];
                    const destinationsIndex = _.keyBy(destinationFiles, (f) => f.path);

                    async.eachSeries(sourceFiles, (sourceInfo, cb) => {
                        self.emit('file', sourceInfo);

                        const destinationInfo = destinationsIndex[sourceInfo.path];
                        const fileName = path.basename(sourceInfo.path);

                        if (
                            !self.options.skipFileNames.includes(fileName) &&
                            (
                                !destinationInfo ||
                                sourceInfo.size !== destinationInfo.size ||
                                sourceInfo.modificationDate.getTime() > destinationInfo.modificationDate.getTime()
                            )
                        ) {
                            self._processFile(sourceInfo)
                                .then((action) => {
                                    const action = {
                                        path: sourceInfo.path,
                                        action: 'copy'
                                    };
                                    actions.push(action);
                                    self.emit('fileDone', action);
                                    cb(null);
                                })
                                .catch(err => {
                                    if (self.options.stopOnError) {
                                        return cb(err);
                                    } else {
                                        const action = {
                                            path: sourceInfo.path,
                                            action: 'error',
                                            error: err
                                        };
                                        actions.push(action);
                                        self.emit('fileDone', action);
                                        cb(null);
                                    }
                                });
                        } else {
                            const action = {
                                path: sourceInfo.path,
                                action: 'skip'
                            };
                            actions.push(action);
                            self.emit('fileDone', action);
                            process.nextTick(() => cb(null));
                        }
                    }, cb);
                }
            ], (err, actions) => {
                self.emit('syncDone', actions);
                if (err) {
                    return reject(err);
                }
                return resolve();
            });
        });
    }

    _processFile(fileInfo) {
        var readStream = this._sourceContainer.getReadStream(fileInfo.path);

        readStream.on('error', err => {
          console.log('--- read stream error', err);
        });

        return this._destinationContainer.uploadFile(fileInfo.path, readStream, fileInfo.size);
    }
}

module.exports = StorageSync;
