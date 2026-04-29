const Promise = require('bluebird');
// eslint-disable-next-line import/no-extraneous-dependencies
// const _ = require('underscore');
const BaseTransport = require('homie-sdk/lib/Bridge/BasePropertyTransport');
const Deferred = require('../../Deferred/Deferred');
const DPTLib = require('../../knx/src/dptlib');


class KNXTransport extends BaseTransport {
    constructor(config) {
        super({ pollInterval: 0, ...config });
        // bindind handlers~
        this.handleKNXConnected = this.handleKNXConnected.bind(this);
        this.handleKNXDisconnected = this.handleKNXDisconnected.bind(this);
        // ~bindind handlers

        this.gas = {
            read       : (config.read       || config.get) || null,             // array of GAs, read flag, react to GroupValueRead telegram coming from the bus
            transmit   : (config.transmit   || config.set) || null,             // single GA, transmit flag, will for this objects transmit any updated object value
            write      : (config.write      || config.get) || [],               // array of GAs, write flag, react to GroupValueWrite telegram coming from the bus
            update     : (config.update     || config.get) || [],               // array of GAs, update flag, react to GroupValueResponse telegram coming from the bus
            initialize : (config.initialize || config.get[0]) || null           // single GA, initialize flag, will send a GroupValueRead telegram to get initial value
        };
        this.dpt = config.dpt;

        this.handleGroupValue_Write = this.handleGroupValue_Write.bind(this);
        this.handleGroupValue_Response = this.handleGroupValue_Response.bind(this);
        this.handleGroupValue_Read = this.handleGroupValue_Read.bind(this);
        this.handleKNXConnected = this.handleKNXConnected.bind(this);
        this.handleKNXDisconnected = this.handleKNXDisconnected.bind(this);

        this.handleNewData = this.handleNewData.bind(this);
        this.settable = !!this.gas.transmit;
    }
    // sync
    attachBridge(bridge) {
        if (this.bridge) {
            if (bridge === this.bridge) return;
            throw new Error('Another bridge is already attached.');
        }
        super.attachBridge(bridge);
        this.bridge.knxConnection.on('GroupValue_Read', this.handleGroupValue_Read);
        this.bridge.knxConnection.on('GroupValue_Write', this.handleGroupValue_Write);
        this.bridge.knxConnection.on('GroupValue_Response', this.handleGroupValue_Response);
        this.on('afterGet', this.handleNewData);
        this.on('afterSet', this.handleNewData);
        this.bridge.on('knx.connected', this.handleKNXConnected);
        this.bridge.on('knx.disconnected', this.handleKNXDisconnected);
        if (this.bridge.knxConnection.connected) this.startPolling();
    }
    detachBridge() {
        this.bridge.knxConnection.off('GroupValue_Read', this.handleGroupValue_Read);
        this.bridge.knxConnection.off('GroupValue_Write', this.handleGroupValue_Write);
        this.bridge.knxConnection.off('GroupValue_Response', this.handleGroupValue_Response);
        this.off('afterGet', this.handleNewData);
        this.off('afterSet', this.handleNewData);
        this.bridge.off('knx.connected', this.handleKNXConnected);
        this.bridge.off('knx.disconnected', this.handleKNXDisconnected);
        this.stopPolling();
        super.detachBridge();
    }
    // async
    async get() {
        if (this.debug) this.debug.info(`KNXTransport.get ${this.id}`, this.gas.initialize);

        const ga = this.gas.initialize;
        const connection = this.bridge.knxConnection;

        if (!connection.connected) throw new Error('Knx connection is not established.');

        return new Promise(async (resolve, reject) => {
            if (this.debug) this.debug.info(`1 KNXTransport.get ${this.id}`, this.gas.initialize);
            if (this.debug) this.debug.info('KNXTransport.get.1');
            await connection.read(ga, this.dpt, (err, src, value) => {
                if (this.debug) this.debug.info(`2 KNXTransport.get ${this.id}`, this.gas.initialize);
                if (err) return reject(err);
                resolve({ src, value });
            }).catch(reject);
        }).then((result) => {
            if (this.debug) this.debug.info(`3 KNXTransport.get ${this.id}`, this.gas.initialize);
            return result.value;
        }).then((resData) => {
            if (this.debug) this.debug.info(`4 KNXTransport.get ${this.id}`, this.gas.initialize);
            this.emit('connected');
            this.emit('afterGet', resData);

            return resData;
        }, (error) => {
            if (this.debug) this.debug.info(`5 KNXTransport.get ${this.id}`, this.gas.initialize);
            // Do NOT emit('disconnected') on a single read timeout. Many KNX
            // objects are write-only (no GroupValue_Response on Read) — failing
            // to read them is normal and must not flip the homie device $state
            // to 'disconnected'. Device-online status is driven by bus-level
            // KNX (re)connection events (handleKNXConnected/Disconnected) which
            // are the real signal of reachability.
            throw error;
        });
    }
    async set(value) {
        if (this.debug) this.debug.info('KNXTransport.set', this.gas);

        const ga = this.gas.transmit;
        const connection = this.bridge.knxConnection;

        if (!connection.connected) throw new Error('KNX connection is not established.');

        if (this.debug) this.debug.info('KNXTransport.set 1');
        const clear = () => {
            if (this.debug) this.debug.info('KNXTransport.set clear');
            clearTimeout(timeout);
            this.off('GroupValue_Write', handleWrite);
            this.off('afterPoll', handlePoll);
        };
        const deferred = new Deferred();

        // eslint-disable-next-line no-shadow
        const handleWrite = (src, dest, value) => {
            if (this.debug) this.debug.info('KNXTransport.set handleWrite');
            clear();
            this.emit('connected');
            this.emit('afterSet', value);
            deferred.resolve(value);
        };
        // eslint-disable-next-line no-shadow
        const handlePoll = (value) => {
            if (this.debug) this.debug.info('KNXTransport.set handlePoll');
            clear();
            this.emit('connected');
            this.emit('afterSet', value);
            deferred.resolve(value);
        };

        this.once('GroupValue_Write', handleWrite);
        // eslint-disable-next-line prefer-const
        let timeout;

        await connection.write(ga, value, this.dpt);
        deferred.registerTimeout(10000, () => {
            if (this.debug) this.debug.info('KNXTransport.set timeout');
            clear();
            // Do NOT emit('disconnected') on write-confirm timeout. KNX is a
            // best-effort bus; not every write is echoed by GroupValue_Write
            // from the destination device, and a missed echo does not mean the
            // device is offline. Bus-level events drive online status.
        });
        timeout = setTimeout(async () => {
            if (this.debug) this.debug.info('KNXTransport.set.setTimeout');
            this.once('afterPoll', handlePoll);
            this.pulled = false;
            this.poll();
        }, 1000);

        return deferred.promise;
    }
    // KNX handlers start
    async handleGroupValue_Read(src, dest) {
        if (src === (this.bridge.knxConnection.connection.options.physAddr || '15.15.15')) return;
        if (this.debug) this.debug.info('KNXTransport.event.handleGroupValue_Read.', { src, dest });
        if (!this.gas.read.includes(dest)) return;
        if (!this.data) return this.emit('error', new Error(`Receive GroupValue_Read request for GA ${dest}, but data is not yet initialized.`));
        this.emit('GroupValue_Read', src, dest);
        await this.bridge.knxConnection.respond(dest, this.data, this.dpt);
    }
    async handleGroupValue_Write(src, dest, value) {
        if (!this.gas.write.includes(dest)) return;
        if (this.debug) this.debug.info('KNXTransport.event.handleGroupValue_Write.', { src, dest, value });
        try {
            value = DPTLib.fromBuffer(value, DPTLib.resolve(this.dpt));
            this.emit('GroupValue_Write', src, dest, value);
            this.handleNewData(value);
        } catch (e) {
            this.emit('error', e);
        }
    }
    async handleGroupValue_Response(src, dest, value) {
        if (!this.gas.update.includes(dest)) return;
        if (this.debug) this.debug.info('KNXTransport.event.handleGroupValue_Response.', { src, dest, value });
        try {
            value = DPTLib.fromBuffer(value, DPTLib.resolve(this.dpt));
            this.emit('GroupValue_Response', src, dest, value);
            this.handleNewData(value);
        } catch (e) {
            this.emit('error', e);
        }
    }
    // KNX handlers end
    async handleKNXConnected() {
        if (this.debug) this.debug.info('KNXTransport.event.handleKNXConnected');
        this.pulled = false;
        // Bus is back: surface 'connected' so the parent NodeBridge clears the
        // homie device $state from 'disconnected' to 'ready'. Without this,
        // devices stayed offline in UI after any bus blip even when telemetry
        // started flowing again.
        this.emit('connected');
        this.enablePolling();
    }
    async handleKNXDisconnected() {
        if (this.debug) this.debug.info('KNXTransport.event.handleKNXDisconnected');
        this.pulled = false;
        // Bus is gone: this is the only case where the device is genuinely
        // unreachable. Single-property read/write timeouts no longer flip the
        // device $state — only this event does.
        this.emit('disconnected');
        this.disablePolling();
    }
}

module.exports = KNXTransport;
