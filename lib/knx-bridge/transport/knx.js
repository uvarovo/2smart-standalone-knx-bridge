const Promise = require('bluebird');
// eslint-disable-next-line import/no-extraneous-dependencies
// const _ = require('underscore');
const BaseTransport = require('homie-sdk/lib/Bridge/BasePropertyTransport');
const DPTLib = require('../../knx/src/dptlib');


// Process-global "command wins" guard, keyed by group address. Shared between
// this transport's set() and the sidecar (app_set_sidecar.js), which run in the
// same process. Lets a delayed bus echo of the previous value be rejected even
// when the homie property's own /set subscription missed the command (the
// sidecar's independent MQTT client still received it).
const KNX_RECENT_SET = (global.__knxRecentSet = global.__knxRecentSet || {});

// ---- Gentle slow-poll of read-only sensor GAs -------------------------------
// KNX_DISABLE_POLL=1 silences the per-property poll loop (and its re-burst on
// every reconnect) to keep flaky routers' tunnels alive. Side effect: read-only
// sensors that never broadcast spontaneously (many thermostat temperature /
// humidity objects) then never refresh — their homie value freezes at the last
// value seen, so the UI shows stale readings ("gateway not updating").
//
// This optional scheduler re-reads only NON-settable GAs (sensors; relays and
// setpoints are excluded — their state comes from write echoes) at a slow,
// strictly serialized cadence: one GroupValue_Read at a time, spaced by
// KNX_SLOW_POLL_GAP_MS and spread across KNX_SLOW_POLL_MS, so there is never a
// burst. Reads already queue behind writes (SEND_GAP + write-priority in
// KnxConnection) and a read that goes unanswered just times out after 3 s in
// the knx lib — it does NOT force a tunnel reconnect — so this is safe to run
// alongside KNX_DISABLE_POLL. Disabled unless KNX_SLOW_POLL_MS > 0, so default
// behaviour is unchanged for bridges that don't set it.
const SLOW_POLL_MS      = parseInt(process.env.KNX_SLOW_POLL_MS, 10) || 0;
const SLOW_POLL_GAP_MS  = parseInt(process.env.KNX_SLOW_POLL_GAP_MS, 10) || 500;
const SLOW_POLL_READ_MS = parseInt(process.env.KNX_SLOW_POLL_READ_TIMEOUT_MS, 10) || 6000;
const slowPollRegistry  = new Set();
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

let slowPollStarted = false;

async function slowPollLoop() {
    // eslint-disable-next-line no-constant-condition
    while (true) {
        const started = Date.now();

        for (const t of Array.from(slowPollRegistry)) {
            const conn = t.bridge && t.bridge.knxConnection;

            if (conn && conn.connected && t.gas && t.gas.initialize) {
                try {
                    // get() emits afterGet -> handleNewData -> publishes to MQTT
                    // on its own; race a hard cap so one wedged read can never
                    // stall the whole cycle.
                    await Promise.race([ t.get(), sleep(SLOW_POLL_READ_MS) ]);
                } catch (e) {
                    // read timeout / write-only GA / mid-poll disconnect: skip
                    // silently, no reconnect is forced.
                }
            }
            await sleep(SLOW_POLL_GAP_MS);
        }

        const elapsed = Date.now() - started;

        await sleep(Math.max(SLOW_POLL_GAP_MS, SLOW_POLL_MS - elapsed));
    }
}

function startSlowPoll() {
    if (slowPollStarted || SLOW_POLL_MS <= 0) return;
    slowPollStarted = true;
    slowPollLoop();
    console.log(`[SLOW-POLL] enabled: interval=${SLOW_POLL_MS}ms gap=${SLOW_POLL_GAP_MS}ms`);
}

class KNXTransport extends BaseTransport {
    static recentSet(ga, value) {
        if (ga) KNX_RECENT_SET[ga] = { value, ts: Date.now() };
    }
    static recentSetGet(ga) {
        return ga ? KNX_RECENT_SET[ga] : undefined;
    }
    constructor(config) {
        // KNX_DISABLE_POLL disables the on-(re)connect GroupValueRead burst.
        // Each reconnect otherwise re-reads every property (pulled=false +
        // enablePolling), and on routers that do not reliably ACK read
        // TUNNELING_REQUESTs (zavod :3699 / new Dom) those reads time out and
        // force yet another reconnect — a self-sustaining read storm that tears
        // the tunnel down ~1x/second even while completely idle, so relay writes
        // keep landing in the dead window. With polling off the tunnel stays
        // quiet between commands; state is still tracked from spontaneous device
        // broadcasts and from the echo of our own writes.
        const pollOff = process.env.KNX_DISABLE_POLL === '1';
        super({ pollInterval: pollOff ? null : 0, ...config });
        this.pollDisabled = pollOff;
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
        // Read-only sensors (no settable GA) are the ones that go stale under
        // KNX_DISABLE_POLL; register them for the gentle slow-poll (no-op unless
        // KNX_SLOW_POLL_MS is set).
        if (SLOW_POLL_MS > 0 && !this.gas.transmit && this.gas.initialize) {
            slowPollRegistry.add(this);
            startSlowPoll();
        }
    }
    detachBridge() {
        slowPollRegistry.delete(this);
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

        // Resolve optimistically as soon as the write is queued onto the bus,
        // instead of waiting up to 10 s for the GroupValue_Write echo. The echo
        // (when the destination device acts on the telegram) still arrives via
        // the persistent handleGroupValue_Write listener and updates state
        // through handleNewData if the actual value differs from what we set.
        // Blocking on the echo added 50–2000 ms of perceived UI lag per relay
        // click, made worse by an extra GroupValueRead probe at +1 s; that
        // probe in turn added bus traffic, which made echoes even slower —
        // a feedback loop that visibly stalled the UI on busy installs.
        // Publish the new state optimistically and IMMEDIATELY, before (and
        // independently of) the bus write. On the flaky zavod/new Dom routers a
        // write can be rejected while queued ('Timeout' when the FSM is busy, or
        // 'Connection closed' during a reconnect) — which used to make set()
        // throw before ever emitting 'afterSet', so the homie state silently
        // stayed on the old value and the UI toggle "didn't take" even though
        // the relay physically switched. Reliable bus delivery is guaranteed
        // separately by the sidecar's confirm-and-retry loop (it re-issues the
        // write until the router returns an L_Data.con), so treating the click
        // as applied here is accurate in practice and keeps the UI in sync.
        // Remember the last commanded value so a delayed bus echo of the OLD
        // value cannot revert it (see handleGroupValue_Write). Recorded in a
        // process-global map keyed by GA so that the sidecar — which reliably
        // receives EVERY /set even when this homie property's own subscription
        // missed it — can update the same guard (app_set_sidecar.js).
        KNXTransport.recentSet(ga, value);

        this.emit('connected');
        this.emit('afterSet', value);

        try {
            await connection.write(ga, value, this.dpt);
        } catch (e) {
            if (this.debug) this.debug.info('KNXTransport.set write deferred to sidecar retry', { ga, message: e && e.message });
        }

        return value;
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
    // Central "command wins" guard for every state update this transport
    // publishes. handleNewData is the single choke point through which state
    // reaches homie/MQTT — from afterSet (optimistic), afterGet (poll) and bus
    // echoes (GroupValue_Write/Response). On the flaky zavod/new Dom installs
    // the homie property's own /set subscription is intermittently slow and
    // out-of-order, so its afterSet can fire LATE with the PREVIOUS command's
    // value and overwrite the freshly set state (the UI toggle visibly snaps
    // back). The sidecar's independent MQTT client receives every command
    // promptly and records it in the shared recentSet guard, so we drop any
    // update that contradicts a command issued within the last
    // KNX_SET_GUARD_MS. Updates that agree, and any change after the guard
    // window (e.g. a genuine external switch), are applied normally.
    handleNewData(value, force) {
        // In sidecar mode the main homie process's own MQTT client is the flaky
        // one: it intermittently misses /set commands AND delivers its state
        // publishes seconds late and out of order, so a lagged publish of the
        // previous value lands after the sidecar has already published the new
        // one and visibly reverts the UI toggle. The in-process guard below
        // runs promptly and cannot help, because the lag is downstream in the
        // MQTT layer. For settable properties we therefore let the sidecar
        // worker — whose independent MQTT client receives every command and
        // publishes state promptly and in order — be the SOLE publisher, and we
        // only keep this.data in sync here without emitting to MQTT.
        if (this.pollDisabled && this.gas.transmit) {
            this.data = value;
            return;
        }
        const rec = KNXTransport.recentSetGet(this.gas.transmit);
        if (rec) {
            const guardMs = parseInt(process.env.KNX_SET_GUARD_MS, 10) || 5000;
            if ((Date.now() - rec.ts) < guardMs && value !== rec.value) return;
        }
        return super.handleNewData(value, force);
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
        // Bus is back: surface 'connected' so the parent NodeBridge clears the
        // homie device $state from 'disconnected' to 'ready'. Without this,
        // devices stayed offline in UI after any bus blip even when telemetry
        // started flowing again.
        this.emit('connected');
        // When polling is disabled we must NOT reset pulled / re-enable polling
        // on every reconnect, otherwise the read storm described in the
        // constructor comes right back.
        if (this.pollDisabled) return;
        this.pulled = false;
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
