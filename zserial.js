
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
            } else {
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
                return;
            } // do nothing unless we have a payload

            setCallback(msg);

            // 是否队列发送
            if (msg.hasOwnProperty("queueSend") && msg.queueSend === true) {
                curPort.enqueue(msg, node, function (err, res) {
                    if (err) {
                        node.error(err);
                    }
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
                });
            }
        }

        function setCallback(msg) {
            let curPort = serialPool.get(msg.serialConfig);
            // 确保只绑定一次事件
            if (curPort._isBindOnOutEventInit) {
                // if (done) done();
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
            setCallback();
        }

        this.on("input", function (msg, send, done) {
            onMsg(msg, send, done);
        })



        function setCallback() {
            let conns = serialPool.curConnections();
            let connKeys = Object.keys(conns) || [];
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
                    //node.status({ fill: "green", shape: "dot", text: "node-red:common.status.connected" });
                    node.status({ fill: "green", shape: "dot", text: "当前连接数：" + connKeys.length });

                });
                curPort.on('closed', function () {
                    //node.status({ fill: "red", shape: "ring", text: "node-red:common.status.not-connected" });
                    node.status({ fill: "green", shape: "dot", text: "当前连接数：" + connKeys.length });

                });
                curPort.on('stopped', function () {
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
            if (!msg.hasOwnProperty("payload")) { return; } // do nothing unless we have a payload
            if (msg.hasOwnProperty("count") && (typeof msg.count === "number") && (msg.serialConfig.out === "count")) {
                msg.serialConfig.newline = msg.count;
            }
            if (msg.hasOwnProperty("flush") && msg.flush === true) { curPort.serial.flush(); }
            let statusText = `waiting：${curPort.serial.path}`
            node.status({ fill: "yellow", shape: "dot", text: "serial.status.waiting:" + curPort.serial.path });
            curPort.enqueue(msg, node, function (err, res) {
                if (err) {
                    node.error(err)
                }
            });

            setCallback(msg, done);
        }
        function setCallback(msg, done) {
            let curPort = serialPool.get(msg.serialConfig);
            // 确保只绑定一次事件
            if (curPort._isBindEventInit) {
                // if (done) done();
                return;
            }
            // node.warn("setCallback called for " + curPort.serial.path);
            curPort._isBindEventInit = true;

            curPort.on('data', function (msgout, sender) {
                // node.warn("对象绑定：："+ node == sender);
                // serial request will only process incoming data pertaining to its own request (i.e. when it's at the head of the queue)
                if (sender !== node) { return; }
                node.status({ fill: "green", shape: "dot", text: "node-red:common.status.ok" });
                msgout.status = "OK";
                node.send(msgout);
                // if (done) done();
            });
            curPort.on('timeout', function (msgout, sender) {
                if (sender !== node) { return; }
                msgout.status = "ERR_TIMEOUT";
                node.status({ fill: "red", shape: "ring", text: "serial.status.timeout" });
                node.send(msgout);
                // if (done) done();
            });
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
                    newline = "" + (serialConfig.newline || '\\n'),
                    spliton = serialConfig.out || 'char',
                    waitfor = serialConfig.waitfor || '',
                    binoutput = serialConfig.bin || 'false',
                    addchar = serialConfig.addchar || '',
                    responsetimeout = serialConfig.responsetimeout || 10000;
                var id = port;
                // just return the connection object if already have one
                // key is the port (file path)
                if (connections[id]) { return connections[id]; }

                // State variables to be used by the on('data') handler
                var i = 0; // position in the buffer
                // .newline is misleading as its meaning depends on the split input policy:
                //   "char"  : a msg will be sent after a character with value .newline is received
                //   "time"  : a msg will be sent after .newline milliseconds
                //   "count" : a msg will be sent after .newline characters
                // if we use "count", we already know how big the buffer will be
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
                        enqueue: function (msg, sender, cb) {
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
                                msgout.port = port;
                                // if we have some leftover stuff, just send it
                                if (i !== 0) {
                                    var m = buf.slice(0, i);
                                    m = Buffer.from(m);
                                    i = 0;
                                    if (binoutput !== "bin") { m = m.toString(); }
                                    msgout.payload = m;
                                }
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
                    obj._emitter.setMaxListeners(50);
                    var olderr = "";
                    var setupSerial = function () {
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
                                if (err.toString() !== olderr) {
                                    olderr = err.toString();
                                    RED.log.error("Err1:[serialconfig:" + serialConfig.id + "] " + RED._("serial.errors.error", { port: port, error: olderr }), {});
                                }
                                obj.tout = setTimeout(function () {
                                    setupSerial();
                                }, serialReconnectTime);
                            }
                            // RED.log.error("init:::::::::::::::::connNumChange");
                            _zemitter.emit('connNumChange', Object.keys(connections).length)
                        });
                        obj.serial.on('error', function (err) {
                            RED.log.error("Err2:[serialconfig:" + serialConfig.id + "] " + RED._("serial.errors.error", { port: port, error: err.toString() }), {});
                            obj._emitter.emit('closed');
                            if (obj.tout) { clearTimeout(obj.tout); }
                            obj.tout = setTimeout(function () {
                                setupSerial();
                            }, serialReconnectTime);
                        });
                        obj.serial.on('close', function () {
                            if (!obj._closing) {
                                if (olderr !== "unexpected") {
                                    olderr = "unexpected";
                                    RED.log.error("Err3:[serialconfig:" + serialConfig.id + "] " + RED._("serial.errors.unexpected-close", { port: port }), {});
                                }
                                obj._emitter.emit('closed');
                                if (obj.tout) { clearTimeout(obj.tout); }
                                obj.tout = setTimeout(function () {
                                    setupSerial();
                                }, serialReconnectTime);
                            }
                            else {
                                obj._emitter.emit('stopped');
                            }
                        });
                        obj.serial.on('open', function () {
                            olderr = "";
                            RED.log.info("[serialconfig:" + serialConfig.id + "] " + RED._("serial.onopen", { port: port, baud: baud, config: databits + "" + parity.charAt(0).toUpperCase() + stopbits }));
                            // Set flow control pins if necessary. Must be set all in same command.
                            var flags = {};
                            if (dtr != "none") { flags.dtr = (dtr != "low"); }
                            if (rts != "none") { flags.rts = (rts != "low"); }
                            if (cts != "none") { flags.cts = (cts != "low"); }
                            if (dsr != "none") { flags.dsr = (dsr != "low"); }
                            if (dtr != "none" || rts != "none" || cts != "none" || dsr != "none") { obj.serial.set(flags); }
                            if (obj.tout) { clearTimeout(obj.tout); obj.tout = null; }
                            //obj.serial.flush();
                            obj._emitter.emit('ready');
                        });

                        obj.serial.on('data', function (d) {
                            RED.log.info("data::::" + d);
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
