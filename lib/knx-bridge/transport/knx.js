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

// ---- Gentle slow-poll of read-only sensor group addresses -------------------
// KNX_DISABLE_POLL=1 silences the per-property poll loop (and its re-burst on
// every reconnect) to keep flaky routers' tunnels alive. Side effect: read-only
// sensors that never broadcast spontaneously — thermostat actual-temperature /
// humidity objects being the typical case — then never refresh, so their homie
// value freezes at whatever was last seen and the UI shows stale readings.
//
// This scheduler re-reads ONLY non-settable GAs (relays and setpoints are
// excluded: their state comes from write echoes) at a slow, strictly serialized
// cadence — one GroupValue_Read at a time, spaced by KNX_SLOW_POLL_GAP_MS, so
// there is never a burst. Reads queue behind writes (write priority + SEND_GAP
// in KnxConnection) and an unanswered read merely times out in the knx lib
// without forcing a tunnel reconnect, which is what makes this safe to run
// together with KNX_DISABLE_POLL.
//
// It is ON BY DEFAULT whenever polling is disabled: sensors going stale is the
// direct consequence of KNX_DISABLE_POLL, so the compensation must not depend
// on per-bridge env being present. Set KNX_SLOW_POLL_MS=0 to opt out.
const SLOW_POLL_DEFAULT_MS  = 120000;
const SLOW_POLL_GAP_MS      = parseInt(process.env.KNX_SLOW_POLL_GAP_MS, 10) || 500;
// Just above the knx lib's own 3 s read timeout, so this cap only ever catches a
// read that wedged somewhere else (e.g. queued behind a reconnect).
const SLOW_POLL_READ_MS     = parseInt(process.env.KNX_SLOW_POLL_READ_TIMEOUT_MS, 10) || 4000;
// A GA that never answers a read is write-only. Stop wasting bus time on it
// after this many consecutive failures, but retry once every BACKOFF_CYCLES so
// a sensor that was merely unreachable comes back on its own.
const SLOW_POLL_MAX_FAILS     = parseInt(process.env.KNX_SLOW_POLL_MAX_FAILS, 10) || 5;
const SLOW_POLL_BACKOFF_CYCLES = parseInt(process.env.KNX_SLOW_POLL_BACKOFF_CYCLES, 10) || 10;
// Tunnel-safety guard. On routers that don't reliably ACK read TUNNELING_REQUESTs
// (zavod :3699, new Dom) a read can cost a tunnel reconnect, and a reconnect
// window is exactly when relay commands get lost — the reason polling was turned
// off in the first place. So: never poll while the tunnel is freshly
// (re)connected, and if reconnects keep happening while our reads are in flight,
// suspend the whole slow-poll for a cool-down. Relay reliability always wins;
// stale sensors are the lesser evil.
const SLOW_POLL_STABLE_MS   = parseInt(process.env.KNX_SLOW_POLL_STABLE_MS, 10) || 60000;
const SLOW_POLL_MAX_HARM    = parseInt(process.env.KNX_SLOW_POLL_MAX_HARM, 10) || 3;
const SLOW_POLL_COOLDOWN_MS = parseInt(process.env.KNX_SLOW_POLL_COOLDOWN_MS, 10) || 1800000;

function resolveSlowPollMs() {
    const raw = process.env.KNX_SLOW_POLL_MS;

    if (raw !== undefined && raw !== '') {
        const parsed = parseInt(raw, 10);

        return Number.isNaN(parsed) || parsed < 0 ? SLOW_POLL_DEFAULT_MS : parsed;
    }

    return process.env.KNX_DISABLE_POLL === '1' ? SLOW_POLL_DEFAULT_MS : 0;
}

const SLOW_POLL_MS     = resolveSlowPollMs();
const slowPollRegistry = new Set();
const sleep            = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

let slowPollStarted     = false;
let slowPollCycle       = 0;
let slowPollReadInFlight = false;
let lastDisconnectAt    = 0;
let slowPollHarm        = 0;

// A disconnect that lands while one of our reads is on the wire is attributed to
// the slow-poll — that's the failure mode this guard exists for.
function watchTunnelStability(bridge) {
    if (!bridge || bridge.__slowPollStabilityWatched) return;
    bridge.__slowPollStabilityWatched = true;
    bridge.on('knx.disconnected', () => {
        lastDisconnectAt = Date.now();
        if (slowPollReadInFlight) slowPollHarm++;
    });
}

async function slowPollOnce(transport) {
    const conn = transport.bridge && transport.bridge.knxConnection;

    if (!conn || !conn.connected || !transport.gas || !transport.gas.initialize) return 'skipped';
    // Skip GAs that consistently don't answer (write-only objects), retrying
    // them occasionally instead of every cycle.
    if (transport.slowPollFails >= SLOW_POLL_MAX_FAILS
        && (slowPollCycle % SLOW_POLL_BACKOFF_CYCLES) !== 0) return 'skipped';

    try {
        slowPollReadInFlight = true;
        // get() publishes on its own through afterGet -> handleNewData. Race a
        // hard cap so a single wedged read can never stall the whole cycle.
        const done = await Promise.race([ transport.get().then(() => true), sleep(SLOW_POLL_READ_MS) ]);

        if (!done) throw new Error('slow-poll read timeout');
        transport.slowPollFails = 0;

        return 'ok';
    } catch (e) {
        // Read timeout / write-only GA / mid-poll disconnect: count and move on.
        // No reconnect is forced and nothing is logged per read — a write-only
        // sensor GA would otherwise flood the log every cycle.
        transport.slowPollFails = (transport.slowPollFails || 0) + 1;

        return 'failed';
    } finally {
        slowPollReadInFlight = false;
    }
}

function anyConnected() {
    for (const transport of slowPollRegistry) {
        if (transport.bridge && transport.bridge.knxConnection && transport.bridge.knxConnection.connected) return true;
    }

    return false;
}

async function slowPollLoop() {
    // eslint-disable-next-line no-constant-condition
    while (true) {
        // Don't burn a whole interval on a cycle that would skip everything:
        // right after startup the tunnel needs a couple of seconds, and while
        // the bus is down there is nothing to read.
        // eslint-disable-next-line no-unmodified-loop-condition
        while (!anyConnected()) await sleep(2000);

        if (slowPollHarm >= SLOW_POLL_MAX_HARM) {
            console.error(new Date().toISOString(),
                `[SLOW-POLL] suspended for ${Math.round(SLOW_POLL_COOLDOWN_MS / 1000)}s: ${slowPollHarm} tunnel reconnects happened during sensor reads — relay reliability takes precedence.`);
            slowPollHarm = 0;
            await sleep(SLOW_POLL_COOLDOWN_MS);
            continue;
        }

        const sinceDisconnect = Date.now() - lastDisconnectAt;

        if (sinceDisconnect < SLOW_POLL_STABLE_MS) {
            // Tunnel just came back: stay off the bus until it has proven stable.
            await sleep(SLOW_POLL_STABLE_MS - sinceDisconnect);
            continue;
        }

        const started = Date.now();
        const stats = { ok: 0, failed: 0, skipped: 0 };

        for (const transport of Array.from(slowPollRegistry)) {
            stats[await slowPollOnce(transport)]++;
            await sleep(SLOW_POLL_GAP_MS);
        }

        slowPollCycle++;
        const elapsed = Date.now() - started;

        // One compact line per cycle: enough to tell "sensors are being
        // refreshed" from "the bus is not answering" without reading telegrams.
        console.log(new Date().toISOString(),
            `[SLOW-POLL] cycle=${slowPollCycle} refreshed=${stats.ok} no-answer=${stats.failed} skipped=${stats.skipped} took=${elapsed}ms`);

        await sleep(Math.max(SLOW_POLL_GAP_MS, SLOW_POLL_MS - elapsed));
    }
}

function startSlowPoll() {
    if (slowPollStarted || SLOW_POLL_MS <= 0) return;
    slowPollStarted = true;
    console.log(`[SLOW-POLL] enabled: interval=${SLOW_POLL_MS}ms gap=${SLOW_POLL_GAP_MS}ms`);
    slowPollLoop();
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
        this.slowPollFails = 0;
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
        // Read-only sensors are the ones that go stale while polling is off;
        // register them for the gentle slow-poll.
        if (SLOW_POLL_MS > 0 && !this.gas.transmit && this.gas.initialize) {
            slowPollRegistry.add(this);
            watchTunnelStability(this.bridge);
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
