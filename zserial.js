
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

        function setCallback(msg, serialConfig, done) {
            let curPort = serialPool.get(serialConfig);
            // 确保只绑定一次事件
            if (curPort._isBindEventInit) {
                return;
            }
            // node.warn("setCallback called for " + curPort.serial.path);
            curPort._isBindEventInit = true;

            curPort.on('data', function (msgout, sender) {
                if (sender !== node) { return; }
                msgout.status = "OK";
                zsend(msgout, null, null, curPort.serial.path, done);
            });
            curPort.on('timeout', function (msgout, sender) {
                if (sender !== node) { return; }
                msgout.status = "ERR_TIMEOUT";
                msgout.port = curPort.serial.path;
                node.status({ fill: "red", shape: "ring", text: "timeout:::" + curPort.serial.path });
                zsend(null, msgout, null, curPort.serial.path, done);
            });

            curPort.on('initerror', function (port, retryNum, olderr) {
                // zsend(null, {
                //     status: "ERR_INIT",
                //     text: `请检查端口是否打开,重试次数${retryNum}`,
                //     error: olderr,
                //     port: port
                // }, null, curPort.serial.path, done);
            });


            curPort.on('retryerror', function (port, retryNum) {
                // curPort._retryNum = 0;
                // zsend(null, {
                //     status: "ERR_retry",
                //     text: `重试${retryNum}失败`,
                //     port: port
                // }, null, curPort.serial.path, done);
            });

            curPort.on('closed', function (port) {
                // node.warn(`串口已关闭:${port}`);
            });
            curPort.on('ready', function (port) {
                // node.warn(`串口已准备好:${port}`);
            });
        }

        this.on("input", function (msg, send, done) {

            onMsg(msg, send, done);
        })
        this.on("close", function (done) {
            node.totalMsg = null;
            node.successMsg = null;
            node.errorMsg = null;
            try {
                serialPool.closeAll(done, node);
            } catch (error) {
                done();
            }

        });
    }
    RED.nodes.registerType("zserial batch", SerialBatchRequest);



    var serialPool = (function () {
        var connections = {};
        var _zemitter = new events.EventEmitter();

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
                                    if (binoutput !== "bin") { m = m.toString(); }
                                    msgout.payload = m;
                                } else {
                                    msgout.payload = (binoutput !== "bin") ? "" : Buffer.alloc(0);
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
                                return { ok: false };
                            }
                            if (b.length < 10) return { ok: false }; // 还缺 CTRL/LEN

                            const len = b[9] >>> 0;
                            const total = 12 + len;                    // 帧总长（含 CS 与 0x16）
                            const endIdx = total - 1;                  // 末尾 0x16 索引
                            const csIdx = total - 2;                  // ★ CS 在倒数第2个字节

                            // CS = 从第一个 0x68 起累加到 CS 之前（不含 CS）
                            const csOK = (end) => {
                                const e = end;              // e == total
                                const cs = b[e - 2];         // csIdx
                                const s = sum8(b, 0, e - 2);
                                return cs === s;
                            };

                            // 1) 优先按 LEN 快速命中
                            if (b.length >= total && b[endIdx] === 0x16 && csOK(total)) {
                                return { ok: true, used: feCount + total, frame: input.slice(0, feCount + total) };
                            }

                            // 2) 回溯：向后找 0x16 做候选结尾，再校验 CS（兼容异常 LEN/粘包）
                            for (let end = 12; end <= b.length; end++) {
                                if (b[end - 1] !== 0x16) continue;
                                // 候选帧至少要有 …… + CS + 16，因此 end >= 13 且 csIdx=end-2 >= 0
                                if (end >= 13) {
                                    const cs = b[end - 2];
                                    const s = sum8(b, 0, end - 2);
                                    if (cs === s) {
                                        return { ok: true, used: feCount + end, frame: input.slice(0, feCount + end) };
                                    }
                                }
                            }

                            // 3) 起始形态确定不对则丢 1 字节；否则继续等
                            if (b.length >= 8 && b[7] !== 0x68) {
                                return { ok: false, used: feCount + 1, frame: input.slice(0, feCount + 1), err: "645_BAD_SHAPE_DROP1" };
                            }
                            return { ok: false };
                        }

                        // 698（68-LEN 变体）：68 LL LH C ... DATA ... CS 16 ；兼容 FE* 前导
                        function tryParse698Len(input) {
                            // 剥 FE 前导
                            const b = stripFE(input);
                            if (b.length < 6 || b[0] !== 0x68) return { ok: false };
                            // 避免把 645 误判成 698-LEN
                            if (b.length >= 8 && b[7] === 0x68) return { ok: false };

                            // 在缓冲里寻找候选的 0x16 作为帧尾（支持一包多帧/脏数据）
                            let end = b.indexOf(0x16, 5);
                            while (end !== -1) {
                                // —— ① 先试 2 字节 CRC-16/X.25（低字节在前），计算区间：从 LL 开始到 CRC 前一字节 —— 
                                if (end >= 3) {
                                    const lo = b[end - 2], hi = b[end - 1];
                                    const fcs = (hi << 8) | lo;
                                    const calc = crc16x25(b, /*start=*/1, /*end=*/end - 2); // [LL..CRC-1]
                                    if (calc === fcs) {
                                        return { ok: true, used: end + 1, frame: b.slice(0, end + 1) };
                                    }
                                }

                                // —— ② 再试 1 字节和校验（sum8），计算区间：从 control 起到 CS 前 —— 
                                if (end >= 2) {
                                    const cs = b[end - 1];
                                    const sum = sum8(b, /*start=*/3, /*end=*/end - 1);
                                    if (cs === sum) {
                                        return { ok: true, used: end + 1, frame: b.slice(0, end + 1) };
                                    }
                                }

                                // 找下一个 0x16
                                end = b.indexOf(0x16, end + 1);
                            }

                            // 还不能定论，继续累计字节
                            return { ok: false };
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
                            if (payload.length < 3) return { ok: false, used: endPos + 1, frame: rawFrame, err: "698_HDLC_TOO_SHORT" };
                            const fcsLo = payload[payload.length - 2];
                            const fcsHi = payload[payload.length - 1];
                            const fcs = (fcsHi << 8) | fcsLo;
                            const calc = crc16x25(payload, 0, payload.length - 2);
                            if (calc !== fcs) return { ok: false, used: endPos + 1, frame: rawFrame, err: "698_HDLC_CRC_FAIL" };
                            return { ok: true, used: endPos + 1, frame: rawFrame }; // 原始含 7E
                        }

                        // 统一喂入器：抽帧、报错、剔噪
                        let assembleBuf = Buffer.alloc(0);
                        function feedAndExtract(d, emitOk, emitErr) {
                            if (!Buffer.isBuffer(d)) d = Buffer.from(d);
                            assembleBuf = Buffer.concat([assembleBuf, d]);

                            // 防溢出
                            if (assembleBuf.length > bufMaxSize) {
                                emitErr(Buffer.from(assembleBuf), "BUFFER_OVERFLOW_DROP_OLD");
                                assembleBuf = Buffer.alloc(0);
                                return;
                            }

                            // 前导剔噪：仅保留以 FE/68/7E 开头；若以 FE 开头，剥离所有 FE 前导
                            let s = 0;
                            while (s < assembleBuf.length) {
                                const c = assembleBuf[s];
                                if (c === 0xFE || c === 0x68 || c === 0x7E) break;
                                s++;
                            }
                            if (s > 0) assembleBuf = assembleBuf.slice(s);
                            if (assembleBuf.length && assembleBuf[0] === 0xFE) {
                                let k = 0; while (k < assembleBuf.length && assembleBuf[k] === 0xFE) k++;
                                assembleBuf = assembleBuf.slice(k);
                            }

                            // 抽帧
                            while (assembleBuf.length >= 5) {
                                // 优先 HDLC，再 645，再 698-Len（避免误判）
                                let r = tryParse698HDLC(assembleBuf);
                                if (!r.ok) r = tryParse645(assembleBuf);
                                if (!r.ok) r = tryParse698Len(assembleBuf);

                                if (r.ok) {
                                    emitOk(r.frame);
                                    assembleBuf = assembleBuf.slice(r.used);
                                    continue;
                                }
                                if (r.used) {
                                    emitErr(r.frame, r.err || "FRAME_INVALID");
                                    assembleBuf = assembleBuf.slice(r.used);
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
                                        var m = Buffer.from(frameBuf);
                                        var last_sender = null;
                                        if (obj.queue.length) { last_sender = obj.queue[0].sender; }
                                        var msgout = obj.dequeue() || {};
                                        if (binoutput !== "bin") { m = m.toString(); }
                                        msgout.payload = m;
                                        msgout.port = port;
                                        msgout.status = "OK";
                                        obj._emitter.emit('data', msgout, last_sender);
                                    },
                                    // 错误帧（已有边界但校验/结束符不通过）
                                    function (badBuf, reason) {
                                        var m = Buffer.from(badBuf);
                                        var last_sender = null;
                                        if (obj.queue.length) { last_sender = obj.queue[0].sender; }
                                        var msgout = obj.dequeue() || {};
                                        if (binoutput !== "bin") { m = m.toString(); }
                                        msgout.payload = m;
                                        msgout.port = port;
                                        msgout.status = "ERR_FRAME";
                                        msgout.reason = reason || "FRAME_INVALID";
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
