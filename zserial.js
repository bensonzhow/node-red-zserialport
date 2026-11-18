
module.exports = function (RED) {
    /*jshint -W082 */
    "use strict";
    var settings = RED.settings;
    var events = require("events");
    const { inspect } = require("util");
    const { SerialPort } = require('serialport');
    var bufMaxSize = 32768;  // Max serial buffer size, for inputs...
    const serialReconnectTime = settings.serialReconnectTime || 15000;

    // Keep one listener per node/connection, with the latest request as context.
    function watchInitError(node, connection, msg, serialConfig) {
        if (!node._initErrorWatches) {
            node._initErrorWatches = new Map();
            node.on('close', function () {
                node._initErrorWatches.forEach(function (watch) {
                    watch.connection.off('initerror', watch.handler);
                });
                node._initErrorWatches.clear();
            });
        }
        var port = serialConfig.serialport;
        var watch = node._initErrorWatches.get(port);
        if (watch && watch.connection !== connection) {
            watch.connection.off('initerror', watch.handler);
            node._initErrorWatches.delete(port);
            watch = null;
        }
        if (!watch) {
            watch = { connection: connection };
            watch.handler = function (failedPort, attempt, err) {
                var errorMsg = RED.util.cloneMessage(watch.msg);
                errorMsg.status = 'ERR_INIT';
                errorMsg.port = failedPort;
                errorMsg.serialConfig = RED.util.cloneMessage(watch.serialConfig);
                errorMsg.serialError = {
                    phase: err.phase || 'open',
                    win32Code: err.win32Code, bridgePid: err.bridgePid, openAttemptId: err.openAttemptId,
                    attempt: attempt,
                    name: err.name,
                    message: err.message || String(err),
                    stack: err.stack,
                    code: err.code,
                    errno: err.errno,
                    syscall: err.syscall,
                    details: inspect(err, { showHidden: true, depth: null, colors: false,
                        maxArrayLength: null, maxStringLength: null })
                };
                const detail = err.backend === 'win32-sync'
                    ? ' [attempt=' + attempt + ', bridgePid=' + err.bridgePid + ', openAttemptId=' + err.openAttemptId + ']' : '';
                node.error('[zserial initerror] ' + failedPort + ': ' + errorMsg.serialError.message + detail, errorMsg);
            };
            node._initErrorWatches.set(port, watch);
            connection.on('initerror', watch.handler);
        }
        watch.msg = RED.util.cloneMessage(msg);
        watch.serialConfig = RED.util.cloneMessage(serialConfig);
    }

    function waitForClosing(configs, resume, fail) {
        const pending = configs.map(config => serialPool.curConnections()[config.serialport])
            .filter(connection => connection && connection.closePending)
            .map(connection => connection.closePending);
        if (!pending.length) return false;
        Promise.all(pending).then(resume, fail);
        return true;
    }

    function batchCloseError(errors) {
        if (errors.length <= 1) return errors[0];
        return Object.assign(new Error(errors.map(err => err.message).join('; ')), { errors });
    }

    function SerialCloseAllNode(n) {
        RED.nodes.createNode(this, n);
        var node = this;

        function finishClose(msg, err, done) {
            if (err) {
                const errors = err.errors || [err];
                if (!errors.every(e => e.backend === 'win32-sync' && e.code === 'ERR_BRIDGE_STILL_PRESENT')) {
                    return done(err);
                }
                msg.serialCloseErrors = errors.map(e => ({port:e.port, bridgePid:e.bridgePid, code:e.code, message:e.message}));
                node.error(err, RED.util.cloneMessage(msg));
            }
            node.send(msg);
            done();
        }

        function onMsg(msg, send, done) {
            if (msg.serialConfig) {
                serialPool.close(msg.serialConfig.serialport, (err) => {
                    if (err) {
                        msg.status = "Close Error"
                    } else {
                        msg.status = "Closed Ok"
                    }
                    node.send(msg);
                    done();
                }, node, msg.forceClose !== true);
            }
            else if (msg.serialConfigs) {
                let allLen = msg.serialConfigs.length;
                let total = msg.serialConfigs.length;
                const closeErrors = [];
                try {
                    for (var i = 0; i < allLen; i++) {
                        let serialConfig = msg.serialConfigs[i];
                        serialPool.close(serialConfig.serialport, function (err) {
                            if (err) closeErrors.push(err);
                            total--;
                            if (total <= 0) {
                                finishClose(msg, batchCloseError(closeErrors), done);
                            }
                        }, node, msg.forceClose !== true);
                    }
                } catch (error) {
                    node.error(error, msg);
                    done();
                }
            }
            else {
                serialPool.closeAll((err) => {
                    finishClose(msg, err, done);
                }, node, msg.forceClose !== true);
            }
        }
        this.on("input", function (msg, send, done) {
            onMsg(msg, send, done);
        })

    }
    RED.nodes.registerType("zserial closeAll", SerialCloseAllNode);
    function SerialOutNode(n) {
        RED.nodes.createNode(this, n);
        var node = this;
        function onMsg(msg, send, done) {

            if (!msg.serialConfig) {
                node.error('msg.serialConfig 没有定义,已创建更新波特率使用 msg.baudrate');
                msg.status = "NO_SERIAL_CONFIG";
                node.send(msg);
                done();
                return;
            }
            if (waitForClosing([msg.serialConfig], () => onMsg(msg, send, done), done)) return;
            let curPort = serialPool.get(msg.serialConfig);

            watchInitError(node, curPort, msg, msg.serialConfig);

            if (msg.hasOwnProperty("baudrate") && (!curPort.keepOpen || !msg.hasOwnProperty("payload") && !msg.serialConfigs)) {
                var baud = parseInt(msg.baudrate);
                if (isNaN(baud)) {
                    node.error(RED._("serial.errors.badbaudrate"), msg);
                } else {
                    curPort.update({ baudRate: baud }, function (err, res) {
                        if (err) {
                            node.error(err);
                        }
                    });
                }
            }
            if (!msg.hasOwnProperty("payload")) {
                node.warn(RED._("serial.errors.nopayload"));
                done();
                return;
            } // do nothing unless we have a payload

            setCallback(msg);

            // 是否队列发送
            if (msg.hasOwnProperty("queueSend") && msg.queueSend === true) {
                curPort.enqueue(msg, node, function (err, res) {
                    if (err) {
                        node.error(err);
                    }
                    done();
                });
            } else {
                var payload = curPort.encodePayload(msg.payload);
                curPort.write(payload, function (err, res) {
                    if (err) {
                        node.error(err);
                        msg.status = "ERR_WRITE";
                    } else {
                        msg.status = "OK";
                    }
                    done();
                }, msg);
            }
        }

        function setCallback(msg) {
            let curPort = serialPool.get(msg.serialConfig);
            // 确保只绑定一次事件
            if (curPort._isBindOnOutEventInit) {
                return;
            }
            curPort._isBindOnOutEventInit = true;

            curPort.on('ready', function () {
                node.status({ fill: "green", shape: "dot", text: "node-red:common.status.connected" });
            });
            curPort.on('closed', function () {
                node.status({ fill: "red", shape: "ring", text: "node-red:common.status.not-connected" });
            });
            curPort.on('stopped', function () {
                node.status({ fill: "grey", shape: "ring", text: "serial.status.stopped" });
            });
        }


        this.on("input", function (msg, send, done) {
            onMsg(msg, send, done);
        })


    }
    RED.nodes.registerType("zserial out", SerialOutNode);

    function SerialInNode(n) {
        RED.nodes.createNode(this, n);
        var node = this;
        function onMsg(msg, send, done) {
            setCallback(msg, send, done);
            done();
        }

        this.on("input", function (msg, send, done) {
            onMsg(msg, send, done);
        })

        function getConnection() {
            let conns = serialPool.curConnections();
            let connKeys = Object.keys(conns) || [];
            return { conns, connKeys }
        }

        function setCallback(msg, send, done) {
            let { conns, connKeys } = getConnection();
            // node.warn(connKeys);
            node.status({ fill: "green", shape: "dot", text: "当前连接数：" + connKeys.length });

            connKeys.forEach(function (key) {
                let curPort = conns[key];
                if (!curPort) {
                    return;
                }
                // 确保只绑定一次事件
                if (curPort._isBindOnInEventInit) {
                    // if (done) done();
                    return;
                }
                curPort._isBindOnInEventInit = true;
                curPort.on('data', function (msgout) {
                    node.send(msgout);
                });
                curPort.on('ready', function () {
                    let { conns, connKeys } = getConnection();
                    //node.status({ fill: "green", shape: "dot", text: "node-red:common.status.connected" });
                    node.status({ fill: "green", shape: "dot", text: "当前连接数：" + connKeys.length });

                });
                curPort.on('closed', function () {
                    let { conns, connKeys } = getConnection();
                    //node.status({ fill: "red", shape: "ring", text: "node-red:common.status.not-connected" });
                    node.status({ fill: "green", shape: "dot", text: "当前连接数：" + connKeys.length });

                });
                curPort.on('stopped', function () {
                    let { conns, connKeys } = getConnection();
                    //node.status({ fill: "grey", shape: "ring", text: "serial.status.stopped" });
                    node.status({ fill: "green", shape: "dot", text: "当前连接数：" + connKeys.length });

                });
            });
        }
        let connNumChange = function () {
            // node.warn('connNumChange');
            setCallback();
        };
        try {
            serialPool.on('connNumChange', connNumChange);
        } catch (error) {
            node.error("绑定事件监听器时出错: " + error.toString());
        }

        this.on("close", function (done) {
            try {
                serialPool.off('connNumChange', connNumChange);
                serialPool.closeAll(done, node);
            } catch (error) {
                // node.warn("移除事件监听器时出错: " + error.toString());
                done();
            }
        });


    }
    RED.nodes.registerType("zserial in", SerialInNode);


    // request data and waits for reply
    function SerialRequestNode(n) {
        RED.nodes.createNode(this, n);
        var node = this;
        function onMsg(msg, send, done) {

            if (!msg.serialConfig) {
                node.error('msg.serialConfig 没有定义,已创建更新波特率使用 msg.baudrate');
                done();
                return;
            }
            if (waitForClosing([msg.serialConfig], () => onMsg(msg, send, done), done)) return;
            let curPort = serialPool.get(msg.serialConfig);

            watchInitError(node, curPort, msg, msg.serialConfig);

            if (msg.hasOwnProperty("baudrate") && (!curPort.keepOpen || !msg.hasOwnProperty("payload") && !msg.serialConfigs)) {
                var baud = parseInt(msg.baudrate);
                if (isNaN(baud)) {
                    node.error(RED._("serial.errors.badbaudrate"), msg);
                } else {
                    curPort.update({ baudRate: baud }, function (err, res) {
                        if (err) {
                            var errmsg = err.toString().replace("Serialport", "Serialport " + curPort.serial.path);
                            node.error(errmsg, msg);
                        }
                    });
                }
            }
            if (!msg.hasOwnProperty("payload")) {
                done();
                return;
            } // do nothing unless we have a payload
            if (msg.hasOwnProperty("count") && (typeof msg.count === "number") && (msg.serialConfig.out === "count")) {
                msg.serialConfig.newline = msg.count;
            }
            if (msg.hasOwnProperty("flush") && msg.flush === true) { curPort.serial.flush(); }
            let statusText = `waiting：${curPort.serial.path}`
            node.status({ fill: "yellow", shape: "dot", text: statusText });

            setCallback(msg, done);

            curPort.enqueue(msg, node, function (err, res) {
                if (err) {
                    node.error(err)
                }
            });
        }
        function setCallback(msg, done) {
            let curPort = serialPool.get(msg.serialConfig);
            // 确保只绑定一次事件
            if (curPort._isBindEventInit) {
                return;
            }
            // node.warn("setCallback called for " + curPort.serial.path);
            curPort._isBindEventInit = true;

            curPort.on('data', function (msgout, sender) {
                // node.warn("对象绑定：："+ node == sender);
                // serial request will only process incoming data pertaining to its own request (i.e. when it's at the head of the queue)
                if (sender !== node) { return; }
                node.status({ fill: "green", shape: "dot", text: "ok:::" + curPort.serial.path });
                msgout.status = "OK";
                node.send(msgout);
                if (done) done();
            });
            curPort.on('timeout', function (msgout, sender) {
                if (sender !== node) { return; }
                msgout.status = "ERR_TIMEOUT";
                msgout.port = curPort.serial.path;
                node.status({ fill: "red", shape: "ring", text: "timeout:::" + curPort.serial.path });
                node.send(msgout);
                if (done) done();
            });
            curPort.on('ready', function () {
                node.status({ fill: "green", shape: "dot", text: "connected:::" + curPort.serial.path });
            });
            curPort.on('closed', function () {
                node.status({ fill: "red", shape: "ring", text: "not-connected:::" + curPort.serial.path });
                if (done) done();
            });
            curPort.on('stopped', function () {
                node.status({ fill: "grey", shape: "ring", text: "stopped:::" + curPort.serial.path });
            });
        }

        this.on("input", function (msg, send, done) {
            onMsg(msg, send, done);
        })
        this.on("close", function (done) {
            // node.warn("close serial in node")
            try {
                serialPool.closeAll(done, node);
            } catch (error) {
                done();
            }

        });
    }
    RED.nodes.registerType("zserial request", SerialRequestNode);

    function SerialBatchRequest(n) {
        RED.nodes.createNode(this, n);
        var node = this;
        node.totallenth = 0;
        node.totalMsg = {};
        node.successMsg = {};
        node.errorMsg = {};
        node._msg = null

        function initMsg(msg) {
            node._msg = msg;
            node.totallenth = msg.serialConfigs.length;
            node.totalMsg = {};
            node.successMsg = {};
            node.errorMsg = {};
        }
        function zsend(msg, err, alldone, port, done) {

            let payload = msg || err;
            node._msg.payload = payload;
            node.totalMsg[port] = payload

            if (msg) {
                node.successMsg[port] = msg
                node.send([node._msg, null, null]);
            }
            if (err) {
                if (err.payload) {
                    err.sendPayload = err.payload;
                    err.payload = ''
                }
                node.errorMsg[port] = err
                node.send([null, node._msg, null]);
            }
            sendAll(done);
        }
        function onMsg(msg, send, done) {
            const configs = msg.serialConfigs || [];
            const pending = configs.map(config => serialPool.curConnections()[config.serialport]?.closePending);
            if (pending.some(Boolean)) {
                if (!configs.every(config => config.backend === 'win32-sync')) {
                    Promise.all(pending).then(() => onMsg(msg, send, done), done);
                    return;
                }
                Promise.allSettled(pending).then(results => {
                    const failed = new Map();
                    for (const [index, result] of results.entries()) {
                        if (result.status !== 'rejected') continue;
                        const err = result.reason;
                        if (configs[index].backend !== 'win32-sync' || err.code !== 'ERR_BRIDGE_STILL_PRESENT') {
                            return done(err);
                        }
                        failed.set(configs[index].serialport, err);
                    }
                    runBatch(msg, done, failed);
                }).catch(done);
                return;
            }
            runBatch(msg, done);
        }
        function runBatch(msg, done, failed) {
            initMsg(msg);
            if (!msg.serialConfigs) {
                node.error("需要配置批量配置：msg.serialConfigs");
                // zsend(msg, null, null)
                done();
                return;
            }

            for (var i = 0; i < msg.serialConfigs.length; i++) {
                var serialConfig = msg.serialConfigs[i];
                serialConfig._msgid = msg._msgid + "_" + i;
                const port = serialConfig.serialport;
                if (failed && failed.has(port)) {
                    const err = failed.get(port);
                    zsend(null, {...serialConfig, port, status:'ERR_CLOSE', text:err.message, bridgePid:err.bridgePid}, null, port, done);
                } else {
                    getSerialServer(msg, serialConfig, done);
                }
            }
        }

        function sendAll(done) {

            try {
                let len = Object.keys(node.totalMsg).length;
                if (len == node.totallenth) {
                    let payload = {
                        totalMsg: node.totalMsg,
                        successMsg: node.successMsg,
                        errorMsg: node.errorMsg,
                    }
                    node.send([null, null, Object.assign({}, node._msg, {
                        payload: payload
                    })]);
                    done()
                }
            } catch (error) {
                node.error(error);
                done()
            }

        }
        function getSerialServer(msg, serialConfig, done) {
            let curPort = serialPool.get(serialConfig);
            if (curPort.keepOpen && msg.baudrate !== undefined) {
                serialConfig = {...serialConfig, baudrate:msg.baudrate};
            }
            watchInitError(node, curPort, msg, serialConfig);

            if (msg.hasOwnProperty("baudrate") && (!curPort.keepOpen || !msg.hasOwnProperty("payload") && !msg.serialConfigs)) {
                var baud = parseInt(msg.baudrate);
                if (isNaN(baud)) {
                    // node.error(RED._("serial.errors.badbaudrate"), msg);
                    zsend(null, {
                        ...serialConfig,
                        text: RED._("serial.errors.badbaudrate")
                    }, null, curPort.serial.path, done)
                } else {
                    curPort.update({ baudRate: baud }, function (err, res) {
                        if (err) {
                            // node.error(err);
                            zsend(null, {
                                ...serialConfig,
                                status: "ERR_UPDATE",
                                error: err,
                                text: "更新波特率失败"
                            }, null, curPort.serial.path, done)
                        }
                    });
                }
            }
            if (!serialConfig.hasOwnProperty("payload")) {
                zsend(null, {
                    ...serialConfig,
                    text: "No payload"
                }, null, curPort.serial.path, done)
                return;
            } // do nothing unless we have a payload
            if (msg.hasOwnProperty("count") && (typeof msg.count === "number") && (serialConfig.out === "count")) {
                serialConfig.newline = msg.count;
            }
            if (msg.hasOwnProperty("flush") && msg.flush === true) { curPort.serial.flush(); }

            if (curPort.keepOpen) node._syncBatchDone = done;
            setCallback(msg, serialConfig, done);
            // // msg.payload = serialConfig.payload;
            // setTimeout(function () {
            //     try {
            //         // node.warn(`当前：${curPort.serial.path}打开状态，${curPort.isopen}`);
            //         if (curPort.isopen) {
            //             curPort.enqueue(serialConfig, node, function (err, res) {
            //                 // node.warn("加入队列::" + curPort.serial.path);
            //                 // node.warn(curPort.queue);
            //                 // node.warn(res);
            //                 if (err) {
            //                     node.error(err)
            //                 }
            //             }, function (queue) {
            //                 // node.warn("队列开始发送::");
            //                 // node.warn(queue);
            //             });
            //         } else {
            //             curPort._retryNum = 0
            //             zsend(null, {
            //                 ...serialConfig,
            //                 status: "ERR_IN_QUEUE",
            //                 text: '串口未打开，加入消息队列失败',
            //                 port: curPort.serial.path
            //             }, null, curPort.serial.path, done);
            //         }
            //     } catch (error) {
            //         node.error(error);
            //     }

            // }, 100);
            // 等待端口就绪后再入队，避免固定 100ms 竞态导致失败
            var openTimeout = msg.openTimeout || 5000; // 可通过 msg.openTimeout 覆盖，默认 5s
            var enqueued = false;

            function doEnqueue() {
                if (enqueued) return;
                enqueued = true;
                try {
                    curPort.enqueue(serialConfig, node, function (err, res) {
                        if (err) { node.error(err); }
                    }, function (queue) {
                        // 队列开始发送（可选日志）
                    });
                } catch (error) {
                    node.error(error);
                }
            }

            if (curPort.isopen) {
                doEnqueue();
            } else {
                curPort.once('ready', function () {
                    doEnqueue();
                });
                setTimeout(function () {
                    if (!enqueued) {
                        zsend(null, {
                            ...serialConfig,
                            status: "ERR_IN_QUEUE",
                            text: '串口未在超时时间内就绪，加入消息队列失败',
                            port: (curPort.serial && curPort.serial.path) || serialConfig.serialport
                        }, null, (curPort.serial && curPort.serial.path) || serialConfig.serialport, done);
                    }
                }, openTimeout);
            }

        }

        function isCurNode(msgout, curMsg, _ndoe, sender) {
            // if (sender == node) { return true; }
            if (msgout.request_msgid && msgout.request_msgid.startsWith(curMsg._msgid)) {
                return true
            }
            return false
        }

        function setCallback(msg, serialConfig, done) {
            let curPort = serialPool.get(serialConfig);
            // 确保当前节点只绑定一次事件
            // if (curPort._isBindEventInit) {
            //     return;
            // }
            // node.warn("setCallback called for " + curPort.serial.path);
            // curPort._isBindEventInit = true;

            if (node[`_dataHandler_${curPort.serial.path}`]) {
                return;
            }
            const dataHandler = function (msgout, sender) {
                if (sender !== node) { return; }
                try {
                    msgout.status = "OK";
                    zsend(msgout, null, null, curPort.serial.path, curPort.keepOpen ? node._syncBatchDone : done);
                } catch (error) {
                    node.error(error)
                }

            }

            const timeoutHandler = function (msgout, sender) {
                if (sender !== node) { return; }
                msgout.status = "ERR_TIMEOUT";
                msgout.port = curPort.serial.path;
                node.status({ fill: "red", shape: "ring", text: "timeout:::" + curPort.serial.path });
                zsend(null, msgout, null, curPort.serial.path, curPort.keepOpen ? node._syncBatchDone : done);
            }

            node[`_dataHandler_${curPort.serial.path}`] = dataHandler
            node[`_timeoutHandler_${curPort.serial.path}`] = timeoutHandler
            curPort.on('data', dataHandler);
            curPort.on('timeout', timeoutHandler);

            node.on('close', function () {
                node[`_dataHandler_${curPort.serial.path}`] = null
                node[`_timeoutHandler_${curPort.serial.path}`] = null
                curPort.off('data', dataHandler);
                curPort.off('timeout', timeoutHandler);
            })
        }

        function afterClosed(port){
            node[`_dataHandler_${port}`] = null
            node[`_timeoutHandler_${port}`] = null
        }

        if(!node._afterClosed){
            node._afterClosed = afterClosed;
            serialPool.on('afterClosed', node._afterClosed);
        }

        this.on("input", function (msg, send, done) {
            onMsg(msg, send, done);
        })
        this.on("close", function (done) {
            node.totalMsg = null;
            node.successMsg = null;
            node.errorMsg = null;
            try {
                serialPool.off('afterClosed', node._afterClosed);
                serialPool.closeAll(done, node);
                node._afterClosed = null;
            } catch (error) {
                done();
            }

        });
    }
    RED.nodes.registerType("zserial batch", SerialBatchRequest);



    var serialPool = (function () {
        var connections = {};
        var _zemitter = new events.EventEmitter();
        _zemitter.setMaxListeners(500); // 设置最大监听器数量为500，防止警告
        return {
            on: function (event, callback) { _zemitter.on(event, callback); },
            off: function (event, callback) { _zemitter.off(event, callback); },
            curConnections: function () { return connections; },
            get: function (serialConfig, node) {
                // make local copy of configuration -- perhaps not needed?
                var port = serialConfig.serialport,
                    baud = serialConfig.serialbaud || 57600,
                    databits = serialConfig.databits || 8,
                    parity = serialConfig.parity || 'none',
                    stopbits = serialConfig.stopbits || 1,
                    dtr = serialConfig.dtr || 'none',
                    rts = serialConfig.rts || 'none',
                    cts = serialConfig.cts || 'none',
                    dsr = serialConfig.dsr || 'none',
                    newline = "" + serialConfig.newline,
                    spliton = serialConfig.out || 'char',
                    waitfor = serialConfig.waitfor || '',
                    binoutput = serialConfig.bin || 'false',
                    addchar = serialConfig.addchar || '',
                    responsetimeout = serialConfig.responsetimeout || 10000,
                    retryNum = serialConfig.retryNum || 5;
                var id = port;
                // just return the connection object if already have one
                // key is the port (file path)
                if (connections[id]) { return connections[id]; }

                // State variables to be used by the on('data') handler
                var i = 0; // position in the buffer
                // .newline is misleading as its meaning depends on the split input policy:
                //  - "char"       : send when a character equal to .newline is received
                //  - "time"       : send after .newline milliseconds
                //  - "interbyte"  : send when no byte arrives for .newline milliseconds
                //  - "count"      : send after .newline characters
                //   - "frame"      : (NEW) parse DL/T645 & DL/T698.45 (Len & HDLC) frames; emit on complete frame
                // If "count", we already know how big the buffer will be
                var bufSize = (spliton === "count") ? Number(newline) : bufMaxSize;

                waitfor = waitfor.replace("\\n", "\n").replace("\\r", "\r")
                    .replace("\\t", "\t").replace("\\e", "\e")
                    .replace("\\f", "\f").replace("\\0", "\0"); // jshint ignore:line
                if (waitfor.substr(0, 2) == "0x") { waitfor = parseInt(waitfor, 16); }
                if (waitfor.length === 1) { waitfor = waitfor.charCodeAt(0); }
                var active = (waitfor === "") ? true : false;
                var buf = new Buffer.alloc(bufSize);

                var splitc; // split character
                // Parse the split character onto a 1-char buffer we can immediately compare against
                if (newline.substr(0, 2) == "0x") {
                    splitc = new Buffer.from([newline]);
                }
                else {
                    splitc = new Buffer.from(newline.replace("\\n", "\n").replace("\\r", "\r").replace("\\t", "\t").replace("\\e", "\e").replace("\\f", "\f").replace("\\0", "\0")); // jshint ignore:line
                }
                if (addchar === true) { addchar = splitc; }
                addchar = addchar.replace("\\n", "\n").replace("\\r", "\r").replace("\\t", "\t").replace("\\e", "\e").replace("\\f", "\f").replace("\\0", "\0"); // jshint ignore:line

                if (addchar.substr(0, 2) == "0x") { addchar = new Buffer.from([addchar]); }
                let assembleBuf = Buffer.alloc(0);
                let recoveringFrame = false;
                connections[id] = (function () {
                    var obj = {
                        _emitter: new events.EventEmitter(),
                        _isBindEventInit: false,
                        serial: null,
                        _closing: false,
                        isopen: false,
                        _retryNum: 0,
                        tout: null,
                        queue: [],
                        // 防重复推进：同一次 serial 'data' 回调只允许 dequeue 一次。
                        _rxToken: 0,
                        _rxDequeuedToken: -1,
                        // 发送序号：用于标记本次 writehead 的队首请求。
                        _txSeq: 0,
                        _framePending: null,
                        on: function (a, b) { this._emitter.on(a, b); },
                        off: function (a, b) { this._emitter.off(a, b); },
                        once: function (a, b) { this._emitter.once(a, b); },
                        close: function (cb) {
                            this.serial.close(cb);
                        },
                        encodePayload: function (payload) {
                            if (!Buffer.isBuffer(payload)) {
                                if (typeof payload === "object") {
                                    payload = JSON.stringify(payload);
                                }
                                else {
                                    payload = payload.toString();
                                }
                                if (addchar !== "") { payload += addchar; }
                            }
                            else if (addchar !== "") {
                                payload = Buffer.concat([payload, Buffer.from(addchar)]);
                            }
                            return payload;
                        },
                        write: function (m, cb, request) {
                            const rate = request && (request.baudrate ?? request.serialConfig?.serialbaud ?? request.serialbaud);
                            if (this.keepOpen && rate != null) this.serial.write(m, cb, Number(rate));
                            else this.serial.write(m, cb);
                        },
                        update: function (m, cb) { this.serial.update(m, cb); },
                        enqueue: function (msg, sender, cb, encb) {
                            var payload = this.encodePayload(msg.payload);
                            var qobj = {
                                sender: sender,
                                msg: msg,
                                payload: payload,
                                cb: cb,
                            }
                            this.queue.push(qobj);

                            // If we're enqueing the first message in line,
                            // we shall send it right away
                            if (encb) {
                                encb(this.queue)
                            }
                            if (this.queue.length === 1) {
                                this.writehead();
                            }

                        },
                        writehead: function () {
                            if (!this.queue.length) { return; }
                            var qobj = this.queue[0];
                            qobj._done = false;
                            qobj._txId = (++obj._txSeq);
                            var msg = qobj.msg;
                            var timeout = msg.timeout || responsetimeout;
                            var maxPartialWait = msg.maxPartialWait || serialConfig.maxPartialWait || (timeout * 2);
                            const sentAt = Date.now();
                            var scheduleTimeout = function (delay) {
                                obj.tout = setTimeout(onResponseTimeout, delay);
                            };
                            var onResponseTimeout = function () {
                                obj.tout = null;

                                // 只处理仍然挂在队首的同一个请求；正常回包已推进队列时，该 timeout 失效。
                                if (!obj.queue || !obj.queue.length) { return; }
                                if (obj.queue[0] !== qobj) { return; }
                                if (qobj._done) { return; }

                                if (spliton === "frame" && obj._framePending) {
                                    var now = Date.now();
                                    var startedAt = Math.max(sentAt, obj._framePending.since);
                                    var waited = now - startedAt;
                                    const remaining = Math.min(maxPartialWait - waited, sentAt + Number(timeout) + Number(maxPartialWait) - now);
                                    if (remaining > 0) {
                                        scheduleTimeout(Math.min(Math.max(50, timeout), remaining));
                                        return;
                                    }
                                }

                                qobj._done = true;

                                // // if we have some leftover stuff, just send it
                                // if (i !== 0) {
                                //     var m = buf.slice(0, i);
                                //     m = Buffer.from(m);
                                //     i = 0;
                                //     if (binoutput !== "bin") { m = m.toString(); }
                                //     msgout.payload = m;
                                // }
                                // Prefer flushing data depending on split mode
                                var m = null;
                                if (spliton === "frame") {
                                    if (assembleBuf.length) m = assembleBuf;
                                    assembleBuf = Buffer.alloc(0);
                                    recoveringFrame = false;
                                    obj._framePending = null;
                                } else {
                                    if (i !== 0) {
                                        m = buf.slice(0, i);
                                        i = 0;
                                    }
                                }

                                var msgout = obj.dequeue(true) || {};
                                msgout.port = id;
                                if (m) {
                                    if (spliton === "frame") {
                                        const mbuf = Buffer.from(m);
                                        msgout.partial_payload = mbuf;
                                        msgout.partial_payload_hex = mbuf.toString('hex').toUpperCase();
                                        msgout.payload = Buffer.alloc(0);
                                        msgout.payload_hex = "";
                                        msgout.reason = "FRAME_TIMEOUT_PARTIAL";
                                    } else {
                                        if (binoutput !== "bin") { m = m.toString(); }
                                        msgout.payload = m;
                                    }
                                } else {
                                    if (spliton === "frame") {
                                        msgout.payload = Buffer.alloc(0);
                                        msgout.payload_hex = "";
                                    } else {
                                        msgout.payload = (binoutput !== "bin") ? "" : Buffer.alloc(0);
                                    }
                                }
                                msgout.status = "ERR_TIMEOUT";
                                /* Notify the sender that a timeout occurred */
                                obj._emitter.emit('timeout', msgout, qobj.sender);
                            };
                            scheduleTimeout(timeout);
                            this.write(qobj.payload, qobj.cb, qobj.msg);
                        },
                        dequeue: function (ignoreRxTokenGuard) {
                            // if we are trying to dequeue stuff from an
                            // empty queue, that's an unsolicited message
                            if (!this.queue.length) { return null; }

                            // 同一次 serial 'data' 回调内只允许一次出队，避免粘包/误判让队列连续推进。
                            if (!ignoreRxTokenGuard) {
                                if (typeof this._rxToken === 'number' && this._rxDequeuedToken === this._rxToken) {
                                    return null;
                                }
                                if (typeof this._rxToken === 'number') {
                                    this._rxDequeuedToken = this._rxToken;
                                }
                            }

                            var qobj = this.queue[0];
                            if (qobj && qobj._done !== true) { qobj._done = true; }

                            var msg = Object.assign({}, qobj.msg);
                            msg = Object.assign(msg, {
                                request_payload: msg.payload,
                                request_msgid: msg._msgid,
                            });
                            delete msg.payload;
                            if (obj.tout) {
                                clearTimeout(obj.tout);
                                obj.tout = null;
                            }
                            this.queue.shift();
                            this.writehead();
                            return msg;
                        },
                    }
                    //newline = newline.replace("\\n","\n").replace("\\r","\r");
                    obj._emitter.setMaxListeners(500);
                    var olderr = "";
                    obj.syncBackend = serialConfig.backend === "win32-sync";
                    obj.keepOpen = obj.syncBackend && (serialConfig.syncKeepOpen ?? settings.zserialSyncKeepOpen) === true;
                    var setupSerial = function () {
                        if (obj.syncBackend && obj._closing) return;
                        // Keep ownership of the failed adapter until its child really exits.
                        if (obj.syncBackend && obj.serial && obj.serial.bridgePresent) {
                            if (!obj.retryAfterExit) {
                                obj.retryAfterExit = true;
                                obj.serial.once('bridgeReleased', () => {
                                    obj.retryAfterExit = false;
                                    if (!obj._closing) setupSerial();
                                });
                            }
                            return;
                        }
                        obj._retryNum++;
                        // RED.log.info(obj._retryNum)
                        if (obj._retryNum > retryNum) {
                            // serialPool.zlog("已经重试" + retryNum + "次，请检查串口是否正常！", {});
                            // obj._emitter.emit('retryerror', id, retryNum);
                            return null;
                        }
                        const PortImpl = serialConfig.backend === 'win32-sync'
                            ? require('./lib/win32-sync-port') : SerialPort;
                        obj.serial = new PortImpl({
                            path: port,
                            baudRate: baud,
                            dataBits: databits,
                            parity: parity,
                            stopBits: stopbits,
                            //parser: serialp.parsers.raw,
                            ...(obj.syncBackend && (serialConfig.syncDiagnostics ?? settings.zserialSyncDiagnostics) === true
                                ? { diagnosticDir: require('path').join(settings.userDir || process.cwd(), 'zserial-diagnostics') } : {}),
                            autoOpen: true
                        }, function (err, results) {
                            if (obj.syncBackend && obj._closing) return;
                            if (err) {
                                RED.log.error("[zserial initerror] " + inspect({
                                    port: port,
                                    attempt: obj._retryNum,
                                    maxAttempts: retryNum,
                                    baudRate: baud,
                                    dataBits: databits,
                                    parity: parity,
                                    stopBits: stopbits,
                                    error: err
                                }, {
                                    showHidden: true,
                                    depth: null,
                                    colors: false,
                                    maxArrayLength: null,
                                    maxStringLength: null
                                }));
                                obj._emitter.emit('initerror', id, obj._retryNum, err);
                                if (err.toString() !== olderr) {
                                    olderr = err.toString();
                                }
                                var delay = serialReconnectTime + Math.floor(Math.random() * 1000);
                                obj.tout = setTimeout(function () {
                                    setupSerial();
                                }, delay);
                                // obj.tout = setTimeout(function () {
                                //     setupSerial();
                                // }, serialReconnectTime);
                            } else {
                                obj.isopen = true;
                            }
                            _zemitter.emit('connNumChange', Object.keys(connections).length)
                        });
                        obj.serial.on('error', function (err) {
                            if (obj.syncBackend && obj._closing) return;
                            obj.isopen = false;
                            serialPool.zlog("Err2:[serialconfig:" + id + "] " + RED._("serial.errors.error", { port: port, error: err.toString() }), {});
                            obj._emitter.emit('closed', id);
                            if (obj.tout) { clearTimeout(obj.tout); }
                            // obj.tout = setTimeout(function () {
                            //     setupSerial();
                            // }, serialReconnectTime);
                            var delay = serialReconnectTime + Math.floor(Math.random() * 1000);
                            obj.tout = setTimeout(function () {
                                setupSerial();
                            }, delay);
                        });
                        obj.serial.on('close', function () {
                            obj.isopen = false;
                            assembleBuf = Buffer.alloc(0);
                            recoveringFrame = false;
                            obj._framePending = null;
                            if (!obj._closing) {
                                if (olderr !== "unexpected") {
                                    olderr = "unexpected";
                                    serialPool.zlog("Err3:[serialconfig:" + id + "] " + RED._("serial.errors.unexpected-close", { port: port }), {});
                                }
                                obj._emitter.emit('closed', id);
                                if (obj.tout) { clearTimeout(obj.tout); }
                                // obj.tout = setTimeout(function () {
                                //     setupSerial();
                                // }, serialReconnectTime);
                                var delay = serialReconnectTime + Math.floor(Math.random() * 1000);
                                obj.tout = setTimeout(function () {
                                    setupSerial();
                                }, delay);
                            }
                            else {
                                obj._emitter.emit('stopped', id);
                            }
                        });
                        obj.serial.on('open', function () {
                            olderr = "";
                            serialPool.zlog("[serialconfig:" + serialConfig.id + "] " + RED._("serial.onopen", { port: port, baud: baud, config: databits + "" + parity.charAt(0).toUpperCase() + stopbits }));
                            // Set flow control pins if necessary. Must be set all in same command.
                            var flags = {};
                            if (dtr != "none") { flags.dtr = (dtr != "low"); }
                            if (rts != "none") { flags.rts = (rts != "low"); }
                            if (cts != "none") { flags.cts = (cts != "low"); }
                            if (dsr != "none") { flags.dsr = (dsr != "low"); }
                            if (dtr != "none" || rts != "none" || cts != "none" || dsr != "none") { obj.serial.set(flags); }
                            if (obj.tout) { clearTimeout(obj.tout); obj.tout = null; }
                            //obj.serial.flush();
                            obj.isopen = true;
                            obj._emitter.emit('ready', id);
                        });


                        /***** -------------------------------- Frame parsers for DL/T645 & DL/T698.45 (FULL) -------------------------------- *****/
                        // FE 去前导（645/部分场景）
                        function stripFE(buf) {
                            let s = 0;
                            while (s < buf.length && buf[s] === 0xFE) s++;
                            return buf.slice(s);
                        }
                        // 8bit 累加和（[start,end)）
                        function sum8(buf, start, end) {
                            let sum = 0;
                            for (let i = start; i < end; i++) sum = (sum + buf[i]) & 0xFF;
                            return sum;
                        }
                        // CRC-16/X25：init 0xFFFF, poly 0x1021（反射实现：0x8408），xorout 0xFFFF，帧内低字节在前
                        function crc16x25(buf, start, end) {
                            let crc = 0xFFFF;
                            for (let i = start; i < end; i++) {
                                crc ^= buf[i];
                                for (let b = 0; b < 8; b++) {
                                    if (crc & 1) crc = (crc >>> 1) ^ 0x8408;
                                    else crc >>>= 1;
                                }
                            }
                            crc ^= 0xFFFF;
                            return crc & 0xFFFF;
                        }
                        // HDLC 透明传输反转义：0x7D 0xXX -> 0xXX ^ 0x20
                        function unescapeHDLC(src) {
                            const out = [];
                            for (let i = 0; i < src.length; i++) {
                                const b = src[i];
                                if (b === 0x7D && i + 1 < src.length) {
                                    out.push(src[i + 1] ^ 0x20);
                                    i++;
                                } else {
                                    out.push(b);
                                }
                            }
                            return Buffer.from(out);
                        }


                        const bleHeader = Buffer.from([0x7E, 0x7E, 0x7E, 0x5A]);

                        function tryParseBleGNW(buf) {
                            const proto = 'ble-gnw';
                            if (buf.length < 5) return { proto, pending: true };
                            const length = buf[4] + 3;
                            if (length < 9) return { proto, err: 'BLE_LEN_MISMATCH' };
                            if (buf.length < length) return { proto, pending: true };
                            const frame = buf.subarray(0, length);
                            if (frame[length - 2] !== 0x7E || frame[length - 1] !== 0xA5) {
                                return { proto, frame, err: 'BLE_BAD_END' };
                            }
                            if (sum8(frame, 0, length - 3) !== frame[length - 3]) {
                                return { proto, frame, err: 'BLE_CS_FAIL' };
                            }
                            return { proto, frame };
                        }

                        function tryParse645(buf) {
                            const proto = '645';
                            if (buf.length < 8) return { proto, pending: true };
                            if (buf[7] !== 0x68) return {};
                            if (buf.length < 10) return { proto, pending: true };
                            const length = 12 + buf[9];
                            if (buf.length < length) return { proto, pending: true };
                            const frame = buf.subarray(0, length);
                            if (frame[length - 1] !== 0x16) return { proto, frame, err: '645_BAD_END' };

                            const cs = frame[length - 2];
                            const full = sum8(frame, 0, length - 2);
                            if (cs === full) return { proto, frame, meta: { cs_mode: 'full', cs_calc: full } };

                            // Legacy devices in this deployment also use a checksum over C, L and DATA.
                            const legacy = sum8(frame, 8, length - 2);
                            if (cs === legacy) return { proto, frame, meta: { cs_mode: 'std', cs_calc: legacy } };
                            return { proto, frame, err: '645_CS_FAIL' };
                        }

                        function tryParse698Len(buf) {
                            const proto = '698-len';
                            if (buf.length < 5) return { proto, pending: true };
                            const rawLength = buf.readUInt16LE(1);
                            if (rawLength & 0x8000) return {};
                            const units = (rawLength & 0x4000) ? 1024 : 1;
                            const length = (rawLength & 0x3FFF) * units + 2;
                            const fcsStart = length - 3;
                            const control = buf[3];

                            // The draft carries HCS after M, while the deployed 2017 layout carries
                            // it after SA and CA. The existing decoder also accepts a two-byte CA.
                            const firstHcs = 7 + (buf[4] & 0x0F);
                            const hcsOffsets = (control & 0x04) ? [firstHcs] : [firstHcs, firstHcs + 1];
                            let headerPending = false;
                            let headerValid = false;
                            for (const offset of hcsOffsets) {
                                if (offset + 2 > fcsStart) continue;
                                if (buf.length < offset + 2) {
                                    headerPending = true;
                                } else if (crc16x25(buf, 1, offset) === buf.readUInt16LE(offset)) {
                                    headerValid = true;
                                    break;
                                }
                            }
                            if (!headerValid) return headerPending ? { proto, pending: true } : {};
                            if (buf.length < length) return { proto, pending: true, trusted: true };

                            const frame = buf.subarray(0, length);
                            if (frame[length - 1] !== 0x16) return { proto, frame, trusted: true, err: '698_LEN_BAD_END' };
                            const fcs = frame.readUInt16LE(fcsStart);
                            const standard = crc16x25(frame, 1, fcsStart);
                            const legacy = crc16x25(frame, 0, fcsStart);
                            const valid = fcs === standard || fcs === legacy;
                            return {
                                proto, frame, trusted: true,
                                err: valid ? undefined : '698_LEN_FCS_FAIL',
                                meta: { fcs_ok: valid, fcs_frame: fcs, fcs_calc_a: legacy, fcs_calc_b: standard }
                            };
                        }

                        function tryParseHDLC(buf) {
                            const proto = '698-hdlc';
                            const end = buf.indexOf(0x7E, 1);
                            if (end === -1) return buf.length <= bufMaxSize ? { proto, pending: true } : { proto, err: 'HDLC_TOO_LONG' };
                            if (end === 1) return {};
                            const frame = buf.subarray(0, end + 1);
                            const payload = unescapeHDLC(frame.subarray(1, end));
                            if (payload.length < 3) return { proto, frame, err: '698_HDLC_TOO_SHORT' };
                            const fcsStart = payload.length - 2;
                            if (crc16x25(payload, 0, fcsStart) !== payload.readUInt16LE(fcsStart)) {
                                return { proto, frame, err: '698_HDLC_CRC_FAIL' };
                            }
                            return { proto, frame };
                        }

                        function parseFrame(buf, expectedProto) {
                            if (buf[0] === 0x7E) {
                                const prefix = buf.subarray(0, bleHeader.length);
                                if (prefix.equals(bleHeader.subarray(0, prefix.length))) return tryParseBleGNW(buf);
                                return tryParseHDLC(buf);
                            }
                            if (buf[0] !== 0x68) return {};
                            const meter = tryParse645(buf);
                            const exchange = tryParse698Len(buf);
                            if (expectedProto === '645' && meter.frame && !meter.err) return meter;
                            if (exchange.err && meter.frame && !meter.err) return meter;
                            if (exchange.trusted) return exchange;
                            if (exchange.pending && (expectedProto === '698-len' || meter.err)) return exchange;
                            if (meter.frame || meter.pending) return meter;
                            return exchange;
                        }

                        function is645Reply(frame, request) {
                            const control = frame[8];
                            if (!(control & 0x80) || (control & 0x1F) !== (request[8] & 0x1F)) return false;
                            for (let i = 1; i <= 6; i++) {
                                if (request[i] !== 0xAA && frame[i] !== request[i]) return false;
                            }
                            // Normal read replies carry the request's DI. Error replies carry an error byte.
                            if (!(control & 0x40) && [0x11, 0x12].includes(control & 0x1F)) {
                                return frame[9] >= 4 && request[9] >= 4 &&
                                    frame.subarray(10, 14).equals(request.subarray(10, 14));
                            }
                            return true;
                        }

                        function findValidFrame(buf, expectedProto) {
                            for (let i = 1; i < buf.length; i++) {
                                if (buf[i] !== 0x68 && buf[i] !== 0x7E) continue;
                                const candidate = parseFrame(buf.subarray(i), expectedProto);
                                if (candidate.frame && !candidate.err) return i;
                            }
                            return 0;
                        }

                        function feedAndExtract(d, request, emitOk, emitErr) {
                            assembleBuf = Buffer.concat([assembleBuf, d]);
                            const sent = Buffer.isBuffer(request?.payload) ? stripFE(request.payload) : null;
                            const parsed = sent ? parseFrame(sent) : null;
                            const expected = parsed?.frame && !parsed.err && parsed.frame.length === sent.length ? parsed.proto : undefined;
                            const expectedProto = expected === '645' && (sent[8] & 0x80) ? undefined : expected;
                            while (assembleBuf.length) {
                                let start = 0;
                                while (start < assembleBuf.length && assembleBuf[start] !== 0x68 && assembleBuf[start] !== 0x7E) start++;
                                assembleBuf = assembleBuf.subarray(start);
                                if (!assembleBuf.length) break;
                                const result = parseFrame(assembleBuf, expectedProto);
                                const { proto, frame, err, pending, meta } = result;
                                if (pending) {
                                    // After a rejected boundary, a marker inside the bad frame
                                    // can look like a partial header and hide the following frame.
                                    const next = recoveringFrame ? findValidFrame(assembleBuf, expectedProto) : 0;
                                    if (next) {
                                        assembleBuf = assembleBuf.subarray(next);
                                        continue;
                                    }
                                    obj._framePending = {
                                        proto,
                                        since: obj._framePending?.proto === proto ? obj._framePending.since : Date.now()
                                    };
                                    return;
                                }
                                obj._framePending = null;

                                if (!frame || err) {
                                    // A corrupt length may overlap a following frame. Resume one byte later.
                                    assembleBuf = assembleBuf.subarray(1);
                                    if (err) {
                                        recoveringFrame = true;
                                        if (frame) emitErr(frame, err);
                                    }
                                    continue;
                                }
                                recoveringFrame = false;
                                assembleBuf = assembleBuf.subarray(frame.length);
                                if (proto === '645' && !(frame[8] & 0x80) && sent && frame.equals(sent)) continue;
                                const related = (!expectedProto || proto === expectedProto) &&
                                    (expectedProto !== '645' || is645Reply(frame, sent));
                                emitOk(frame, { ...meta, proto }, related ? request : null);
                            }
                            obj._framePending = null;
                            recoveringFrame = false;
                        }

                        /***** -------------------------------- Frame parsers for DL/T645 & DL/T698.45 (FULL) End -------------------------------- *****/


                        obj.serial.on('data', function (d) {
                            obj._rxToken = (obj._rxToken || 0) + 1;
                            // RED.log.info("data::::" + d);
                            function emitData(data) {
                                if (active === true) {
                                    var m = Buffer.from(data);
                                    var last_sender = null;
                                    if (obj.queue.length) { last_sender = obj.queue[0].sender; }
                                    if (binoutput !== "bin") { m = m.toString(); }
                                    var msgout = obj.dequeue() || {};
                                    msgout.payload = m;
                                    msgout.port = port;
                                    obj._emitter.emit('data', msgout, last_sender);
                                }
                                active = (waitfor === "") ? true : false;
                            }

                            // —— 新增：frame 模式（645/698 全覆盖），完整帧即回，错帧带原始数据与原因 —— 
                            if (spliton === "frame") {
                                feedAndExtract(
                                    d,
                                    obj.queue[0],
                                    // 完整 OK 帧
                                    function (frameBuf, meta, request) {
                                        // frame 模式下，务必保持 Buffer 原样输出（避免 0x00 等字节在字符串链路中被截断/损坏）
                                        // 同时附带 payload_hex 便于日志/排障。
                                        var m = Buffer.from(frameBuf);
                                        const msg = request && obj.queue[0] === request ? obj.dequeue() : null;
                                        const msgout = msg || {};
                                        msgout.payload = m;
                                        msgout.payload_hex = m.toString('hex').toUpperCase();
                                        msgout.port = port;
                                        msgout.status = "OK";
                                        msgout.frame_meta = meta;
                                        if (!msg) msgout.is_unsolicited = true;
                                        obj._emitter.emit('data', msgout, msg ? request.sender : null);
                                    },
                                    // 错误帧（已有边界但校验/结束符不通过）
                                    function (badBuf, reason) {
                                        // 错误帧同样保持 Buffer 输出，并提供 HEX 字符串，便于定位截断点/前导位置
                                        var m = Buffer.from(badBuf);
                                        var msgout = {};
                                        msgout.payload = m;
                                        msgout.payload_hex = m.toString('hex').toUpperCase();
                                        msgout.port = port;
                                        msgout.status = "ERR_FRAME";
                                        msgout.reason = reason || "FRAME_INVALID";
                                        msgout.is_unsolicited = !(obj.queue && obj.queue.length);
                                        // 错误帧不绑定请求 sender，也不 dequeue；队首请求继续等完整帧或超时。
                                        obj._emitter.emit('data', msgout, null);
                                    }
                                );
                                return; // 已处理
                            }

                            // —— 其余兼容模式（time/interbyte/count/char）保持原逻辑 —— 
                            // -------- existing legacy split modes (time/interbyte/count/char) --------

                            for (var z = 0; z < d.length; z++) {
                                var c = d[z];
                                if (c === waitfor) { active = true; }
                                if (!active) { continue; }
                                // handle the trivial case first -- single char buffer
                                if ((newline === 0) || (newline === "")) {
                                    emitData(new Buffer.from([c]));
                                    continue;
                                }

                                // save incoming data into local buffer
                                buf[i] = c;
                                i += 1;

                                // do the timer thing
                                if (spliton === "time" || spliton === "interbyte") {
                                    // start the timeout at the first character in case of regular timeout
                                    // restart it at the last character of the this event in case of interbyte timeout
                                    if ((spliton === "time" && i === 1) ||
                                        (spliton === "interbyte" && z === d.length - 1)) {
                                        // if we had a response timeout set, clear it:
                                        // we'll emit at least 1 character at some point anyway
                                        if (obj.tout) {
                                            clearTimeout(obj.tout);
                                            obj.tout = null;
                                        }
                                        obj.tout = setTimeout(function () {
                                            obj.tout = null;
                                            emitData(buf.slice(0, i));
                                            i = 0;
                                        }, newline);
                                    }
                                }
                                // count bytes into a buffer...
                                else if (spliton === "count") {
                                    newline = serialConfig.newline;
                                    if (i >= parseInt(newline)) {
                                        emitData(buf.slice(0, i));
                                        i = 0;
                                    }
                                }
                                // look to match char...
                                else if (spliton === "char") {
                                    if ((c === splitc[0]) || (i === bufMaxSize)) {
                                        emitData(buf.slice(0, i));
                                        i = 0;
                                    }
                                }
                            }
                        });
                        // obj.serial.on("disconnect",function() {
                        //     RED.log.error(RED._("serial.errors.disconnected",{port:port}));
                        // });
                    }
                    setupSerial();
                    return obj;
                }());
                return connections[id];
            },
            close: function (port, done, node, flowClose) {
                if (connections[port]) {
                    const connection = connections[port];
                    if (flowClose && connection.keepOpen && connection.serial.isOpen && !connection.closePending) {
                        connection.serial.trace('retain');
                        done();
                        return;
                    }
                    if (connection.syncBackend) {
                        if (!connection.closePending) {
                            connection._closing = true;
                            clearTimeout(connection.tout);
                            const adapter = connection.serial;
                            const release = () => {
                                if (connections[port] !== connection) return;
                                delete connections[port];
                                _zemitter.emit('connNumChange', Object.keys(connections).length);
                                _zemitter.emit('afterClosed', port);
                            };
                            connection.closePending = Promise.resolve().then(() => new Promise((resolve, reject) => {
                                connection.close(err => {
                                    if (err && adapter.bridgePresent) {
                                        // Keep the rejected promise visible to subsequent close/open
                                        // requests. A late native exit releases this exact connection.
                                        adapter.once('bridgeReleased', release);
                                    } else {
                                        release();
                                    }
                                    if (err) reject(err); else resolve();
                                });
                            }));
                        }
                        connection.closePending.then(() => done(), done);
                        return;
                    }
                    if (connections[port].tout != null) {
                        clearTimeout(connections[port].tout);
                    }
                    connections[port]._closing = true;
                    connections[port]._retryNum = 0;
                    connections[port]._isBindOnOutEventInit = false;
                    connections[port]._isBindOnInEventInit = false;
                    connections[port]._isBindEventInit = false;
                    serialPool.zlog(node, "开始执行关闭");
                    try {
                        connections[port].close(function () {
                            serialPool.zlog(node, "关闭成功");
                            _zemitter.emit('afterClosed', port);
                            RED.log.info(RED._("serial.errors.closed", { port: port }), {});
                            done();
                        });
                    }
                    catch (err) {
                        done(err);
                    }
                    delete connections[port];
                    // RED.log.error("close:::::::::::::::::connNumChange");
                    _zemitter.emit('connNumChange', Object.keys(connections).length)
                }
                else {
                    done();
                }
            },
            closeAll(done, node, flowClose) {
                serialPool.zlog(node, "开始关闭所有串口连接");
                serialPool.zlog(node, connections);
                var keys = Object.keys(connections), total = keys.length;
                const closeErrors = [];
                serialPool.zlog(node, "需要关闭的连接数: " + total);

                // 如果没有连接需要关闭，立即完成
                if (keys.length === 0) {
                    serialPool.zlog(node, "没有串口连接需要关闭");
                    done();
                    return;
                }
                try {
                    for (var i = 0; i < keys.length; i++) {
                        serialPool.close(keys[i], function (err) {
                            if (err) closeErrors.push(err);
                            total--;
                            if (total <= 0) {
                                done(batchCloseError(closeErrors));
                                serialPool.zlog(node, "全部关闭完成");
                            }
                        }, node, flowClose);
                    }
                } catch (error) {
                    done(error);
                }

            },
            zlog(node, msg) {
                // node && node.warn(msg);
            }
        }
    }());


}
