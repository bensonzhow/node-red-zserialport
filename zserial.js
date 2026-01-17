
module.exports = function (RED) {
    /*jshint -W082 */
    "use strict";
    var settings = RED.settings;
    var events = require("events");
    const { SerialPort } = require('serialport');
    var bufMaxSize = 32768;  // Max serial buffer size, for inputs...
    const serialReconnectTime = settings.serialReconnectTime || 15000;


    function SerialCloseAllNode(n) {
        RED.nodes.createNode(this, n);
        var node = this;

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
                }, node);
            }
            else if (msg.serialConfigs) {
                let allLen = msg.serialConfigs.length;
                let total = msg.serialConfigs.length;
                try {
                    for (var i = 0; i < allLen; i++) {
                        let serialConfig = msg.serialConfigs[i];
                        serialPool.close(serialConfig.serialport, function (err) {
                            total--;
                            if (total <= 0) {
                                node.send(msg);
                                done();
                            }
                        }, node);
                    }
                } catch (error) {
                    node.error(error, msg);
                    done();
                }
            }
            else {
                serialPool.closeAll(() => {
                    node.send(msg);
                    done();
                }, node);
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
            let curPort = serialPool.get(msg.serialConfig);

            if (msg.hasOwnProperty("baudrate")) {
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
                });
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
            let curPort = serialPool.get(msg.serialConfig);

            if (msg.hasOwnProperty("baudrate")) {
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
                getSerialServer(msg, serialConfig, done);
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

            if (msg.hasOwnProperty("baudrate")) {
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
                    zsend(msgout, null, null, curPort.serial.path, done);
                } catch (error) {
                    node.error(error)
                }

            }

            const timeoutHandler = function (msgout, sender) {
                if (sender !== node) { return; }
                msgout.status = "ERR_TIMEOUT";
                msgout.port = curPort.serial.path;
                node.status({ fill: "red", shape: "ring", text: "timeout:::" + curPort.serial.path });
                zsend(null, msgout, null, curPort.serial.path, done);
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

        function afterClosed(port) {
            node[`_dataHandler_${port}`] = null
            node[`_timeoutHandler_${port}`] = null
        }

        if (!node._afterClosed) {
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
                        on: function (a, b) { this._emitter.on(a, b); },
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
                        write: function (m, cb) { this.serial.write(m, cb); },
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
                            this.write(qobj.payload, qobj.cb);
                            var msg = qobj.msg;
                            var timeout = msg.timeout || responsetimeout;
                            this.tout = setTimeout(function () {
                                this.tout = null;
                                var msgout = obj.dequeue() || {};
                                msgout.port = id;
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
                                    if (typeof assembleBuf !== "undefined" && assembleBuf && assembleBuf.length) {
                                        m = Buffer.from(assembleBuf);
                                        assembleBuf = Buffer.alloc(0); // 清掉残留
                                    }
                                } else {
                                    if (i !== 0) {
                                        m = buf.slice(0, i);
                                        i = 0;
                                    }
                                }

                                if (m) {
                                    if (spliton === "frame") {
                                        const mbuf = Buffer.from(m);
                                        msgout.payload = mbuf;
                                        msgout.payload_hex = mbuf.toString('hex').toUpperCase();
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
                            }, timeout);
                        },
                        dequeue: function () {
                            // if we are trying to dequeue stuff from an
                            // empty queue, that's an unsolicited message
                            if (!this.queue.length) { return null; }
                            var msg = Object.assign({}, this.queue[0].msg);
                            msg = Object.assign(msg, {
                                request_payload: msg.payload,
                                request_msgid: msg._msgid,
                            });
                            delete msg.payload;
                            if (this.tout) {
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
                    var setupSerial = function () {
                        obj._retryNum++;
                        // RED.log.info(obj._retryNum)
                        if (obj._retryNum > retryNum) {
                            // serialPool.zlog("已经重试" + retryNum + "次，请检查串口是否正常！", {});
                            // obj._emitter.emit('retryerror', id, retryNum);
                            return null;
                        }
                        obj.serial = new SerialPort({
                            path: port,
                            baudRate: baud,
                            dataBits: databits,
                            parity: parity,
                            stopBits: stopbits,
                            //parser: serialp.parsers.raw,
                            autoOpen: true
                        }, function (err, results) {
                            if (err) {
                                obj._emitter.emit('initerror', id, obj._retryNum, err);
                                if (err.toString() !== olderr) {
                                    olderr = err.toString();
                                    // RED.log.error("Err1:[serialconfig:" + id + "] " + RED._("serial.errors.error", { port: port, error: olderr }), {});
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


                        // GNW 蓝牙帧：7E 7E 7E 5A  ...  CS  7E A5
                        function tryParseBleGNW(input) {
                            const b = input;
                            if (b.length < 9) return { ok: false };                   // 最短：4起始 + 1LEN + 1CMD + 0DATA + 1CS + 2结束
                            // 形态判定：必须以 7E 7E 7E 5A 开头
                            if (!(b[0] === 0x7E && b[1] === 0x7E && b[2] === 0x7E && b[3] === 0x5A)) {
                                // 若不是，看看后面是否有候选起点（容错脏字节）
                                const n = b.indexOf(0x7E);
                                if (n > 0) return { ok: false, used: n, frame: b.slice(0, n), err: "BLE_NO_7E7E7E5A_AT_START" };
                                return { ok: false };
                            }

                            // 向后找结束 A5，要求倒数第二个字节是 0x7E（结束符 7E A5）
                            let endPos = -1;
                            for (let i = 5; i < b.length; i++) {
                                if (b[i] === 0xA5 && b[i - 1] === 0x7E) { endPos = i; break; }
                            }
                            if (endPos === -1) return { ok: false };                  // 半包，继续累计

                            const frame = b.slice(0, endPos + 1);                     // [0 .. A5]
                            // 基本字段位置：len 在 [4]，cmd 在 [5]，CS 在倒数第3个字节
                            const L = frame[4] >>> 0;
                            const cs = frame[frame.length - 3] >>> 0;

                            // （宽松）长度一致性：实测总长 T 与 L 的关系为 T = L + 3
                            const T = frame.length;
                            if ((T - 3) !== L) {
                                // 不强制失败，给出“长度不一致”的错误帧提示并丢掉该段，避免卡死
                                return { ok: false, used: frame.length, frame, err: "BLE_LEN_MISMATCH" };
                            }

                            // 和校验：从起始 0x7E 到 CS 之前所有字节累加低8位
                            let sum = 0;
                            for (let i = 0; i < frame.length - 3; i++) sum = (sum + (frame[i] >>> 0)) & 0xFF;
                            if (sum !== cs) {
                                return { ok: false, used: frame.length, frame, err: "BLE_CS_FAIL" };
                            }

                            return { ok: true, used: frame.length, frame };
                        }
                        // 645：FE* 68 + 6 addr + 68 + ctrl + len + data + cs + 16
                        function tryParse645(input) {
                            // 统计 FE 前导（仅用于 used，不参与 CS 计算）
                            let feCount = 0;
                            while (feCount < input.length && input[feCount] === 0xFE) feCount++;
                            const b = input.slice(feCount);
                            if (b.length < 12) return { ok: false };

                            // 形态：68 + addr(6) + 68 + CTRL + LEN + DATA + CS + 16
                            if (b[0] !== 0x68) {
                                const n = b.indexOf(0x68, 1);
                                if (n > 0) return { ok: false, used: feCount + n, frame: input.slice(0, feCount + n), err: "645_NO_68_AT_START" };
                                return { ok: false };
                            }
                            if (b.length >= 8 && b[7] !== 0x68) {
                                const n = b.indexOf(0x68, 1);
                                if (n > 0) return { ok: false, used: feCount + n, frame: input.slice(0, feCount + n), err: "645_SECOND_68_NOT_FOUND" };
                                // 已经有足够字节判定形态错误，但又找不到下一个 0x68：同步点错位，丢 1 字节推进
                                return { ok: false, used: feCount + 1, frame: input.slice(0, feCount + 1), err: "645_BAD_SHAPE_DROP1" };
                            }
                            if (b.length < 10) return { ok: false }; // 还缺 CTRL/LEN

                            const len = b[9] >>> 0;
                            const total = 12 + len;                    // 帧总长（含 CS 与 0x16）
                            const endIdx = total - 1;                  // 末尾 0x16 索引
                            const csIdx = total - 2;                  // ★ CS 在倒数第2个字节

                            // CS 双模校验：
                            //  - full：从第一个 0x68 起累加到 CS 前（不含 CS）
                            //  - std ：从第二个 0x68 后（C+L+DATA）累加到 CS 前
                            const csOK = (end) => {
                                const e = end;               // e == total
                                const cs = b[e - 2];         // CS 在倒数第2字节

                                // full 模式
                                const sFull = sum8(b, 0, e - 2);
                                if (cs === sFull) return { ok: true, mode: 'full', calc: sFull };

                                // std 模式（需确保第二个 0x68 存在于固定位置7）
                                if (b.length >= 10 && b[7] === 0x68) {
                                    // 第二个 0x68 后开始：CTRL(8) + LEN(9) + DATA...
                                    const sStd = sum8(b, 8, e - 2);
                                    if (cs === sStd) return { ok: true, mode: 'std', calc: sStd };
                                    return { ok: false, mode: 'std', calc: sStd, calc_full: sFull };
                                }
                                return { ok: false, mode: 'full', calc: sFull };
                            };

                            // 1) 优先按 LEN 快速命中
                            if (b.length >= total && b[endIdx] === 0x16) {
                                const ck = csOK(total);
                                if (ck && ck.ok) {
                                    const frame = input.slice(0, feCount + total);
                                    // 透出校验模式，便于排障：部分设备使用 std 模式
                                    try {
                                        frame._meta = Object.assign({}, frame._meta || {}, {
                                            proto: '645',
                                            cs_mode: ck.mode,
                                            cs_calc: ck.calc
                                        });
                                    } catch (e) { }
                                    return { ok: true, used: feCount + total, frame };
                                }
                            }

                            // 2) 回溯：向后找 0x16 做候选结尾，再校验 CS（兼容异常 LEN/粘包）
                            for (let end = 12; end <= b.length; end++) {
                                if (b[end - 1] !== 0x16) continue;
                                // 候选帧至少要有 …… + CS + 16，因此 end >= 13 且 csIdx=end-2 >= 0
                                if (end >= 13) {
                                    const ck = csOK(end);
                                    if (ck && ck.ok) {
                                        const frame = input.slice(0, feCount + end);
                                        try {
                                            frame._meta = Object.assign({}, frame._meta || {}, {
                                                proto: '645',
                                                cs_mode: ck.mode,
                                                cs_calc: ck.calc
                                            });
                                        } catch (e) { }
                                        return { ok: true, used: feCount + end, frame };
                                    }
                                }
                            }

                            // 3) 起始形态确定不对则丢 1 字节；否则继续等
                            if (b.length >= 8 && b[7] !== 0x68) {
                                return { ok: false, used: feCount + 1, frame: input.slice(0, feCount + 1), err: "645_BAD_SHAPE_DROP1" };
                            }
                            return { ok: false };
                        }

                        // 698（68-LEN 变体）：68 LL LH C ... DATA ... FCS(2) 16 ；兼容 FE* 前导
                        function tryParse698Len(input) {
                            // 统计 FE 前导（用于 used/frame 回到原始 input）
                            let feCount = 0;
                            while (feCount < input.length && input[feCount] === 0xFE) feCount++;
                            const b = input.slice(feCount);
                            if (b.length < 6 || b[0] !== 0x68) return { ok: false };

                            // ---- 读取长度域 ----
                            // 68 [LL] [LH] [C] ... [FCS(lo)] [FCS(hi)] 16
                            const LL = b[1] >>> 0;
                            const LH = b[2] >>> 0;

                            // Lraw 含单位位/保留位：bit14 为单位位（0=字节，1=KB）
                            let Lraw = (LH << 8) | LL;
                            const isKB = (Lraw & 0x4000) !== 0;
                            Lraw &= 0x3FFF; // 清掉单位位/保留位，只留下数值

                            // 绝大多数场景长度单位为“字节”；若遇到 KB 单位则折算
                            const L = isKB ? (Lraw << 10) : Lraw; // KB -> *1024

                            // ------------------------ 关键防误判（核心修复点） ------------------------
                            // 误判根因：645 帧形态为 68 + 6字节地址 + 68 ...，地址字节中“偶然出现 0x16”
                            // 例如：68 02 80 16 00 00 00 68 ...
                            // 若把 LL=0x02、LH=0x80 解读为 698-LEN 长度，则 L=2，expectedEnd=3，b[3]=0x16
                            // 从而被 698-LEN 误判为 “68 02 80 16” 一帧，导致吞掉 645 的帧头并产生残片。

                            // 约束 1：698-LEN 的 L 不可能极短（至少应包含：C + 头字段 + FCS(2)）
                            // 这里给经验下限 6（偏保守，既能挡住 L=2，又尽量不误伤极端设备）
                            // 若现场仍有碰撞，可把下限提高到 8 或 10（建议先保留 6）。
                            const MIN_698_LEN_L = 10;
                            if (L < MIN_698_LEN_L) {
                                // 不消费任何字节，让后续 parser（如 645）继续尝试
                                return { ok: false };
                            }


                            // 长度上限：防止错位/噪声把 L 解读成极大值导致 assembleBuf 误判为半包长期等待
                            // 掉电次数等业务回包通常远小于 2KB；超出则高度可疑，丢 1 字节推进重同步
                            const MAX_698_LEN_L = 2048;
                            if (L > MAX_698_LEN_L) {
                                return { ok: false, used: feCount + 1, frame: input.slice(0, feCount + 1), err: "698_LEN_TOO_LARGE" };
                            }

                            // 约束 2：控制域 C 在 b[3]，不可能是 0x16（结束符）
                            // 若 b[3] == 0x16，几乎必然是 645 地址域碰撞导致的误判
                            if (b.length >= 4 && b[3] === 0x16) {
                                return { ok: false };
                            }
                            // 约束 3：控制域 C 必须看起来像 698 的控制域（功能码通常为 1=链路管理 或 3=用户数据）
                            // 目的：避免 645 的地址字节被当成 698 控制域，从而把 645 整帧误判为 698-LEN 候选帧并被消费掉。
                            // 698 控制域：bit0..bit3 为功能码（1 或 3 最常见）；其他位为 DIR/PRM/分帧/扰码标志。
                            const C = b[3] >>> 0;
                            const func = C & 0x0F;
                            if (!(func === 0x01 || func === 0x03)) {
                                // 不消费任何字节，让 645 解析器继续尝试
                                return { ok: false };
                            }
                            // ------------------------------------------------------------------------

                            // 期望 0x16 的位置（相对 b 起点）：1 + L
                            // 解释：L 表示从 LL 开始到 FCS(含) 之前的长度？各厂实现略有差异
                            // 你现有逻辑采用 expectedEnd = 1 + L，并要求 b[expectedEnd] == 0x16
                            const expectedEnd = 1 + L;

                            // 半包：继续累积
                            if (b.length < expectedEnd + 1) return { ok: false };

                            // 若 expectedEnd 位置不是 0x16，说明起点错位或存在脏字节
                            // 这里消费 1 字节，避免死循环卡住
                            if (b[expectedEnd] !== 0x16) {
                                return { ok: false, used: feCount + 1, frame: input.slice(0, feCount + 1), err: "698_LEN_BAD_END" };
                            }

                            // ------------------------ FCS 校验（CRC-16/X.25） ------------------------
                            // FCS 位于 0x16 前两字节，低字节在前（lo, hi）
                            if (expectedEnd - 2 < 0) return { ok: false };

                            const fcsLo = b[expectedEnd - 2];
                            const fcsHi = b[expectedEnd - 1];
                            const fcs = (fcsHi << 8) | fcsLo;

                            // 不同实现对“CRC 覆盖范围”存在差异，常见两种：
                            // A) 从 0x68 开始算到 FCS 前（不含 FCS 与 0x16）
                            // B) 从 LL 开始算到 FCS 前（不含 FCS 与 0x16）
                            const calcA = crc16x25(b, 0, expectedEnd - 2);
                            const calcB = crc16x25(b, 1, expectedEnd - 2);
                            const fcsOK = (calcA === fcs) || (calcB === fcs);

                            // 现场经常遇到：帧内容完整（68..16 边界正确），但 FCS 因链路噪声/串口转换器问题偶发不一致。
                            // 若此处直接判为错误帧并 dequeue，会导致上层“偶尔无法解码”（尤其是请求-响应严格匹配的场景）。
                            // 因此：当边界与长度域一致时，允许“容错通过”，并在 _meta 中标记 fcs_ok=false 供上层排障。
                            if (!fcsOK) {
                                return {
                                    ok: false,
                                    used: feCount + expectedEnd + 1,
                                    frame: input.slice(0, feCount + expectedEnd + 1),
                                    fcs_ok: false,
                                    fcs_frame: fcs,
                                    fcs_calc_a: calcA,
                                    fcs_calc_b: calcB,
                                    err: "698_LEN_FCS_FAIL"
                                };
                            }

                            // FCS 通过：返回完整帧（含 0x68..0x16）
                            return {
                                ok: true,
                                used: feCount + expectedEnd + 1,
                                frame: input.slice(0, feCount + expectedEnd + 1),
                                fcs_ok: true
                            };
                        }

                        // 698（HDLC）：7E ... [FCS(lo,hi)] 7E，支持 0x7D 转义与 X.25 FCS
                        function tryParse698HDLC(input) {
                            const b = input;
                            if (b.length < 6) return { ok: false };
                            if (b[0] !== 0x7E) return { ok: false };
                            let endPos = -1;
                            for (let i = 1; i < b.length; i++) { if (b[i] === 0x7E) { endPos = i; break; } }
                            if (endPos === -1) return { ok: false };       // 半包
                            const rawFrame = b.slice(0, endPos + 1);
                            const payloadEscaped = b.slice(1, endPos);     // 去 7E
                            const payload = unescapeHDLC(payloadEscaped);
                            // if (payload.length < 3) return { ok: false, used: endPos + 1, frame: rawFrame, err: "698_HDLC_TOO_SHORT" };
                            if (payload.length < 3) return { ok: false };
                            // 严格形态约束：698-HDLC 的“帧格式域”通常以 0xA0/0xA8/0xB0 等开头（高四位为 0xA 或 0xB）。
                            // 若不满足，极可能只是链路噪声/误同步的 0x7E，不应在 frame 模式下被消费掉（否则会 dequeue 错配）。
                            const fmt = payload[0] >>> 0;
                            const hi = fmt & 0xF0;
                            if (!(hi === 0xA0 || hi === 0xB0)) {
                                return { ok: false }; // 不消费，交给 698-LEN/645 再尝试
                            }
                            const fcsLo = payload[payload.length - 2];
                            const fcsHi = payload[payload.length - 1];
                            const fcs = (fcsHi << 8) | fcsLo;
                            const calc = crc16x25(payload, 0, payload.length - 2);
                            // if (calc !== fcs) return { ok: false, used: endPos + 1, frame: rawFrame, err: "698_HDLC_CRC_FAIL" };
                            if (calc !== fcs) return { ok: false };
                            return { ok: true, used: endPos + 1, frame: rawFrame }; // 原始含 7E
                        }

                        // 统一喂入器：抽帧、报错、剔噪
                        let assembleBuf = Buffer.alloc(0);
                        function feedAndExtract(d, emitOk, emitErr) {
                            if (!Buffer.isBuffer(d)) d = Buffer.from(d);
                            assembleBuf = Buffer.concat([assembleBuf, d]);

                            // 修复：部分现场链路/中间层会把原始 8-bit 串口数据“扩展”为 UTF-16LE 形态
                            //      （如 0x7E -> 0x7E00 0x7E00...），导致帧头无法识别。
                            //      需要自动识别并还原原始 8-bit 数据。
                            // (已移除 normalizeUtf16Interleave 修复块)

                            // 抽帧
                            while (assembleBuf.length >= 5) {
                                // // 优先 HDLC，再 645，再 698-Len（避免误判）
                                // let r = tryParse698HDLC(assembleBuf);
                                // if (!r.ok) r = tryParse645(assembleBuf);
                                // if (!r.ok) r = tryParse698Len(assembleBuf);

                                // --fix-20251213-先 BLE（7E7E7E5A…7EA5），再 698-HDLC，再 698-LEN，再 645
                                // 关键：698-LEN 必须优先于 645，否则遇到 698 帧内出现 0x68（如示例报文第7字节）会被 645 误判并卡住
                                let r = tryParseBleGNW(assembleBuf);
                                if (!r.ok) r = tryParse698HDLC(assembleBuf);

                                // ---- 兜底：0x7E 起始已形成候选段（存在第二个 0x7E），但 BLE/HDLC 均无法校验通过 -> 丢 1 字节推进重同步 ----
                                // 仅在候选段“闭合”时执行（有第二个 0x7E），避免误伤半包；并要求 endPos>=5，避免误伤 7E7E/7E7E7E 前导
                                if (!r.ok && assembleBuf.length && assembleBuf[0] === 0x7E) {
                                    const endPos = assembleBuf.indexOf(0x7E, 1);
                                    if (endPos >= 5) {
                                        r = { ok: false, used: 1, frame: assembleBuf.slice(0, 1), err: "DROP_7E_BADFRAME" };
                                    }
                                }


                                if (!r.ok) r = tryParse698Len(assembleBuf);
                                if (!r.ok) r = tryParse645(assembleBuf);

                                if (r.ok) {
                                    // 附加解析元数据（例如 698 FCS 校验结果），供上层排障
                                    if (r && typeof r === 'object') {
                                        try {
                                            r.frame._meta = {
                                                proto: r.proto || undefined,
                                                fcs_ok: (typeof r.fcs_ok === 'boolean') ? r.fcs_ok : undefined,
                                                err: r.err || undefined,
                                                fcs_frame: r.fcs_frame,
                                                fcs_calc_a: r.fcs_calc_a,
                                                fcs_calc_b: r.fcs_calc_b
                                            };
                                        } catch (e) {
                                            // ignore
                                        }
                                    }
                                    emitOk(r.frame);
                                    assembleBuf = assembleBuf.slice(r.used);
                                    continue;
                                }
                                if (r.used) {
                                    // resync/drop1 类（通常 used 很小）不应上报 ERR_FRAME，否则会产生大量重复输出
                                    const used = r.used >>> 0;
                                    const frameBuf = r.frame ? Buffer.from(r.frame) : null;
                                    const frameLen = frameBuf ? frameBuf.length : 0;

                                    // 判定是否为“仅推进同步点”的消费：
                                    //  - used == 1 或 frameLen <= 1：典型 drop1
                                    //  - 或 err 属于已知的 resync 类原因
                                    const errCode = (r && r.err) ? String(r.err) : "";
                                    const isResyncDrop = (used <= 1) || (frameLen <= 1) ||
                                        errCode === "DROP_7E_BADFRAME" ||
                                        errCode === "645_BAD_SHAPE_DROP1" ||
                                        errCode === "698_LEN_BAD_END" ||
                                        errCode === "698_LEN_TOO_LARGE" ||
                                        errCode === "645_SECOND_68_NOT_FOUND";

                                    if (!isResyncDrop) {
                                        // 只有“闭合候选帧但校验/结构失败”的情况才上报 ERR_FRAME
                                        try {
                                            if (r.frame) {
                                                r.frame._meta = Object.assign({}, r.frame._meta || {}, {
                                                    proto: (r.fcs_ok === false) ? '698-len' : (r.proto || undefined),
                                                    fcs_ok: (typeof r.fcs_ok === 'boolean') ? r.fcs_ok : undefined,
                                                    fcs_frame: r.fcs_frame,
                                                    fcs_calc_a: r.fcs_calc_a,
                                                    fcs_calc_b: r.fcs_calc_b,
                                                    err: r.err || "FRAME_INVALID"
                                                });
                                            }
                                        } catch (e) {
                                            // ignore
                                        }
                                        emitErr(r.frame, errCode || "FRAME_INVALID");
                                    }

                                    // 无论是否上报，都必须消费，防止 assembleBuf 卡死
                                    assembleBuf = assembleBuf.slice(used);
                                    continue;
                                }
                                break; // 需要更多数据
                            }
                        }

                        /***** -------------------------------- Frame parsers for DL/T645 & DL/T698.45 (FULL) End -------------------------------- *****/


                        obj.serial.on('data', function (d) {
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
                                    // 完整 OK 帧
                                    function (frameBuf) {
                                        // frame 模式下，务必保持 Buffer 原样输出（避免 0x00 等字节在字符串链路中被截断/损坏）
                                        // 同时附带 payload_hex 便于日志/排障。
                                        var m = Buffer.from(frameBuf);
                                        var last_sender = null;
                                        if (obj.queue.length) { last_sender = obj.queue[0].sender; }
                                        var msgout = obj.dequeue() || {};
                                        msgout.payload = m;
                                        msgout.payload_hex = m.toString('hex').toUpperCase();
                                        msgout.port = port;
                                        msgout.status = "OK";

                                        // 若解析器附带了元数据（例如 698 FCS 校验结果），一并输出
                                        if (frameBuf && frameBuf._meta) {
                                            msgout.frame_meta = frameBuf._meta;
                                            // 便于后续 GC：避免 assembleBuf 长期引用
                                            try { delete frameBuf._meta; } catch (e) { }
                                        }

                                        obj._emitter.emit('data', msgout, last_sender);
                                    },
                                    // 错误帧（已有边界但校验/结束符不通过）
                                    function (badBuf, reason) {
                                        // 错误帧同样保持 Buffer 输出，并提供 HEX 字符串，便于定位截断点/前导位置
                                        var m = Buffer.from(badBuf);
                                        var last_sender = null;
                                        if (obj.queue.length) { last_sender = obj.queue[0].sender; }
                                        // 关键修复：错误帧（CRC/CS/结尾不符）不应 dequeue，否则会把“请求上下文”弹出队列，导致后续正确回包无法匹配
                                        var msgout = (obj.queue && obj.queue.length) ? Object.assign({}, obj.queue[0].msg) : {};
                                        msgout.payload = m;
                                        msgout.payload_hex = m.toString('hex').toUpperCase();
                                        msgout.port = port;
                                        msgout.status = "ERR_FRAME";
                                        msgout.reason = reason || "FRAME_INVALID";
                                        msgout.is_unsolicited = !(obj.queue && obj.queue.length);
                                        obj._emitter.emit('data', msgout, last_sender);
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
            close: function (port, done, node) {
                if (connections[port]) {
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
            closeAll(done, node) {
                serialPool.zlog(node, "开始关闭所有串口连接");
                serialPool.zlog(node, connections);
                var keys = Object.keys(connections), total = keys.length;
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
                            total--;
                            if (total <= 0) {
                                done();
                                serialPool.zlog(node, "全部关闭完成");
                            }
                        }, node);
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
