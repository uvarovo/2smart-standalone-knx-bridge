// eslint-disable-next-line import/no-extraneous-dependencies
// const Promise = require('bluebird');
const BaseBridge = require('homie-sdk/lib/Bridge/Base');
const BaseDeviceBridge = require('homie-sdk/lib/Bridge/BaseDevice');
const KnxConnection = require('./knx_connection');
const DeviceBridge = require('./device');

class KnxBridge extends BaseBridge {
    constructor(config) {
        super({ ...config, device: null });
        // handlers~
        this.handleKNXConnected = this.handleKNXConnected.bind(this);
        this.handleKNXDisconnected = this.handleKNXDisconnected.bind(this);
        // ~handlers
        const debug = this.debug;


        this.knxConnection = new KnxConnection({
            ipAddr         : config.knxConnection.ipAddr,
            ipPort         : config.knxConnection.ipPort,
            physAddr       : config.knxConnection.physAddr,
            forceTunneling : config.knxConnection.forceTunneling,
            localIp        : config.knxConnection.localIp,
            receivePort    : config.knxConnection.receivePort,
            listenPort     : config.knxConnection.listenPort,
            manualConnect  : true,
            handlers       : {
                connected() {
                    console.log(new Date(), 'KnxBridge.knxConnection.events.connected');
                    if (debug) debug.info('KnxBridge.knxConnection.events.connected');
                },
                disconnected() {
                    console.log(new Date(), 'KnxBridge.knxConnection.events.disconnected');
                    if (debug) debug.info('KnxBridge.knxConnection.events.disconnected');
                },
                event(evt, src, dest, value) {
                    console.log(new Date(), 'KnxBridge.knxConnection.events', { evt, src, dest, value });
                    if (debug) debug.info('KnxBridge.knxConnection.events', { evt, src, dest, value });
                }
            }
        }, { debug });
        this.knxConnection.on('connected', this.handleKNXConnected);
        this.knxConnection.on('disconnected', this.handleKNXDisconnected);
        this.knxConnection.on('error', this.handleErrorPropagate);

        if (config.device) {
            let deviceBridge = config.device;

            if (!(deviceBridge instanceof BaseDeviceBridge)) deviceBridge = new DeviceBridge({ ...deviceBridge }, { debug: config.debug });
            this.setDeviceBridge(deviceBridge);
        }
    }
    // Override republish to throttle MQTT messages.
    // With many mappings (>200), the default republish() floods the TCP socket
    // with 1000+ messages synchronously, triggering MaxListeners warnings and
    // stalling the MQTT client.  This version sends in batches of 10 with
    // 100 ms pauses between them.
    async republish() {
        if (!this.homie || !this.homie.publishToBroker) return;
        const topics = this.homie.getTopics ? this.homie.getTopics() : null;

        if (!topics) return;
        const keys = Object.keys(topics);

        console.log('[REPUBLISH] publishing ' + keys.length + ' topics (throttled)');
        const BATCH = 10;

        for (let i = 0; i < keys.length; i += BATCH) {
            const batch = keys.slice(i, i + BATCH);

            for (const k of batch) {
                try { this.homie.publishToBroker(k, topics[k], { retain: true }); } catch (e) { /* skip */ }
            }
            await new Promise((r) => setTimeout(r, 100));
        }
        console.log('[REPUBLISH] done');
    }

    // sync
    init() {
        if (this.debug) this.debug.info('KnxBridge.knx.init');
        super.init();
        this.knxConnection.Connect();
    }
    destroy() {
        this.knxConnection.Disconnect();
        super.destroy();
    }
    // async
    // handlers~
    handleKNXConnected() {
        if (this.debug) this.debug.info('KnxBridge.knx.handleKNXConnected');
        this.emit('knx.connected');
    }
    handleKNXDisconnected() {
        if (this.debug) this.debug.info('KnxBridge.knx.handleKNXDisconnected');
        this.emit('knx.disconnected');
    }
    // ~handlers
}

module.exports = KnxBridge;
