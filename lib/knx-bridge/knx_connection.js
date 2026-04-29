const EventEmitter = require('events');
const Promise = require('bluebird');
const knx = require('../knx');

const DELAY_AFTER_CONNECTION = 2500;
const REQUEST_PENDING_TIMEOUT = 600000;
const SEND_GAP = 80;

class KnxConnection extends EventEmitter {
    constructor(config, options = {}) {
        super();
        this.debug = options.debug;
        this.handleConnected = this.handleConnected.bind(this);
        this.handleDisconnected = this.handleDisconnected.bind(this);
        this.handleTransition = this.handleTransition.bind(this);
        this.connection = knx.Connection(config);

        const reEmitBuilder = (eventname) => {
            return function () {
                this.emit(eventname, ...arguments);
            };
        };
        const reEmitEvents = [ 'GroupValue_Read', 'GroupValue_Response', 'GroupValue_Write',
            'PhysicalAddress_Write',  'PhysicalAddress_Read', 'PhysicalAddress_Response',
            'ADC_Read', 'ADC_Response', 'Memory_Read', 'Memory_Response', 'Memory_Write',
            'UserMemory', 'DeviceDescriptor_Read', 'DeviceDescriptor_Response',
            'Restart', 'OTHER', 'error' ];

        for (const eventName of reEmitEvents) {
            this.connection.on(eventName, reEmitBuilder(eventName).bind(this));
        }
        this.connection.on('connected', this.handleConnected);
        this.connection.on('disconnected', this.handleDisconnected);
        this.connection.on('transition', this.handleTransition);

        this.real_connected = null;
        this.emit_connected_timeout = null;
        this.connected = null;

        this._requests = [];
        this.lastTimeSend = new Date(0);
        // User-initiated traffic (writes — relay toggles, setpoint changes —
        // and responds — answers to other devices' GroupValue_Read) is dispatched
        // ahead of background reads. Without this, an initial-poll burst of N
        // reads on KNX (re)connect would keep a relay click waiting up to
        // N * SEND_GAP before reaching the bus.
        const PRIORITY_OPS = new Set([ 'write', 'respond' ]);

        for (const name of [ 'respond', 'read', 'write' ]) {
            this[name] = async function () {
                const send = () => {
                    clearTimeout(request.timeout);
                    if (!this.connected) {
                        request.reject(Error('Knx connection is not established.'));
                        return;
                    }
                    try {
                        this.connection[name](...arguments);
                        request.resolve();
                    } catch (e) {
                        request.reject(e);
                    }
                };

                const request = {
                    sent     : false,
                    priority : PRIORITY_OPS.has(name),
                    send
                };
                const promise = new Promise((_resolve, _reject) => {
                    request.reject = _reject;
                    request.resolve = _resolve;
                });

                request.timeout = setTimeout(() => {
                    request.reject(new Error('Timeout.'));
                }, REQUEST_PENDING_TIMEOUT);

                this._requests.push(request);

                this._dispatchNextRequest();

                const after = () => {
                    this._requests = this._requests.filter((_request) => request !==  _request);
                    this._dispatchNextRequest();
                };

                return promise.tap(after).tapCatch(after);
            };
        }
    }
    // sync
    _dispatchNextRequest() {
        if (!this.connected) return;
        if (this.connection.state !== 'idle') return;
        const request = this._requests.find((_request) => !_request.sent && _request.priority)
            || this._requests.find((_request) => !_request.sent);

        if (!request) return;

        const delay = SEND_GAP - (new Date() - this.lastTimeSend);

        if (delay > 0) {
            setTimeout(this._dispatchNextRequest.bind(this), delay);
        } else {
            this.lastTimeSend = new Date();
            request.sent = true;
            process.nextTick(request.send);
        }
    }
    Connect() {
        this.connection.Connect();
    }
    Disconnect() {
        this.connection.Disconnect();
    }
    // async
    // handlers~
    async handleConnected() {
        if (this.real_connected) return;
        this.real_connected = true;
        this.emit_connected_timeout = setTimeout(() => {
            this.connected = true;
            this.emit('connected');
        }, DELAY_AFTER_CONNECTION);
    }
    async handleDisconnected() {
        if (this.real_connected === false) return;
        this.real_connected = false;
        clearTimeout(this.emit_connected_timeout);
        this.connected = false;
        this.emit('disconnected');
        while (this._requests.length) {
            const request = this._requests.pop();

            request.reject(new Error('Connection closed.'));
        }
    }
    async handleTransition() {
        if (this.connection.state === 'idle') this._dispatchNextRequest();
    }
    // ~handlers
}

module.exports = KnxConnection;
