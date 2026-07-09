// Sidecar launcher — spawns a child process with an independent MQTT client
// that handles /set commands for all mapped properties and includes an FSM
// watchdog to reset the KNX state machine when it gets stuck.
'use strict';

var cp   = require('child_process');
var path = require('path');
var fs   = require('fs');

function startSetSidecar(bridge) {
    var deviceId = process.env.MQTT_USER || process.env.DEVICE_ID;

    // Build GA map from config
    var gaMap = {};
    try {
        var raw     = JSON.parse(fs.readFileSync('/etc/nodes.config.json', 'utf8'));
        var mapping = raw.extensions && raw.extensions.mapping;
        if (mapping) {
            Object.keys(mapping).forEach(function (key) {
                var t = mapping[key].transport;
                if (!t) return;
                var ga = t.set || (Array.isArray(t.get) ? t.get[0] : t.get);
                if (ga) gaMap[key] = { ga: ga, dpt: t.dpt || 'DPT1.001' };
            });
        }
    } catch (e) {
        console.error('[SIDECAR] config read error:', e.message);
    }

    console.log('[SIDECAR] GA map loaded, entries=' + Object.keys(gaMap).length);

    // Fork the worker
    var child = cp.fork(path.join(__dirname, 'sidecar_worker.js'), [], {
        env: Object.assign({}, process.env, { DEVICE_ID: deviceId })
    });

    child.on('message', function (msg) {
        if (!msg || !msg.topic) return;
        var topic   = msg.topic;
        var payload = msg.payload;

        // Heartbeat passthrough
        if (topic.indexOf('$heartbeat') >= 0) return;

        // Extract property path: sweet-home/<deviceId>/<nodeId>/<propId>/set
        var parts = topic.replace('sweet-home/' + deviceId + '/', '').replace('/set', '').split('/');
        if (parts.length < 2) return;

        var key = parts[0] + '/' + parts[1];
        var entry = gaMap[key];
        if (!entry) return;

        // Write to KNX bus via bridge connection
        var conn = bridge.knxConnection && bridge.knxConnection.connection;
        if (!conn) {
            console.error('[SIDECAR] no KNX connection, cannot write', key);
            return;
        }

        var ga  = entry.ga;
        var dpt = entry.dpt;
        var val = payload;

        // Parse value based on DPT
        if (dpt.indexOf('DPT1') === 0) {
            val = (payload === 'true' || payload === '1') ? true : false;
        } else if (dpt.indexOf('DPT5') === 0 || dpt.indexOf('DPT7') === 0) {
            val = parseInt(payload, 10);
        } else if (dpt.indexOf('DPT9') === 0 || dpt.indexOf('DPT14') === 0) {
            val = parseFloat(payload);
        }

        console.log('[SIDECAR] WRITE ga=' + ga + ' val=' + val + ' fsm=' + conn.state);

        // The homie state topic is published directly by the sidecar worker the
        // moment it receives the command (see sidecar_worker.js) — the worker's
        // MQTT client is the reliable one, whereas the main homie process's
        // client intermittently misses commands and delivers its state publishes
        // late/out of order. Here we only drive the KNX bus write.

        // Update the shared "command wins" guard the KNX transport reads in its
        // handleNewData (same process, same global object). Because the sidecar
        // receives EVERY command, this lets a delayed bus echo of the previous
        // value be dropped even when the homie property's own /set subscription
        // missed this command.
        global.__knxRecentSet = global.__knxRecentSet || {};
        global.__knxRecentSet[ga] = { value: val, ts: Date.now() };

        writeWithConfirm(bridge, ga, val, dpt);
    });

    // Reliable write with confirmation + retry.
    //
    // Some KNX/IP routers (the "zavod" 192.168.0.227:3699 and "new Dom"
    // 192.168.0.88 sites) intermittently fail to acknowledge our outbound
    // TUNNELING_REQUESTs. knx.js retransmits the datagram 3x and then forces a
    // reconnect (resetting the sequence counter) — but the datagram that was
    // in flight is DROPPED. A single conn.write() therefore lands only ~70-90%
    // of the time, so the relay appears to switch "every other time".
    //
    // For boolean (relay) properties the actuator reliably echoes its real
    // state back on the same group address (get == set here), so we can treat
    // that echo as ground truth: fire the write, watch the GA, and re-issue the
    // write until the confirmed value matches the desired one (or we run out of
    // attempts). Relay writes are idempotent, so repeating them is safe.
    var pending = {}; // ga -> cancel fn, so a newer command supersedes an older
    function writeWithConfirm(bridge, ga, val, dpt) {
        var isBool  = dpt.indexOf('DPT1') === 0;
        var MAX     = parseInt(process.env.KNX_WRITE_RETRIES, 10) || 4;
        var GAP     = parseInt(process.env.KNX_WRITE_RETRY_MS, 10) || 1600;
        var desired = val ? 1 : 0;
        var evtName = 'event_' + ga;

        // Supersede any in-flight retry loop for the same GA (latest wins).
        if (pending[ga]) { pending[ga](); delete pending[ga]; }

        var conn = bridge.knxConnection && bridge.knxConnection.connection;
        if (!conn) { console.error('[SIDECAR] no KNX connection, cannot write', ga); return; }

        // Non-boolean values have no simple echo semantics here: write once.
        if (!isBool) {
            try { conn.write(ga, val, dpt); } catch (e) { console.error('[SIDECAR] write error:', e.message); }
            return;
        }

        var attempts = 0;
        var timer    = null;
        var done     = false;
        var sub      = null;
        var subCon   = null;

        function cleanup() {
            done = true;
            if (timer) clearTimeout(timer);
            // conn is a machina FSM: its emitter uses the subscription handle's
            // .off() (there is no Node-style removeListener).
            if (sub && sub.off) sub.off();
            else if (conn.off) conn.off(evtName, onEvt);
            if (subCon && subCon.off) subCon.off();
            else if (conn.off) conn.off('confirmed', onConfirmed);
            if (pending[ga] === cleanup) delete pending[ga];
        }
        // Primary success signal: the router's L_Data.con for our own write,
        // surfaced by the FSM as a 'confirmed' event carrying the original sent
        // datagram. This means the telegram actually reached the bus. (These
        // relays do NOT broadcast an independent status telegram, so the value
        // echo below never fires for them — the delivery confirmation is the
        // only ground truth we get.)
        function onConfirmed(dg) {
            if (done) return;
            var dest = dg && dg.cemi && dg.cemi.dest_addr;
            if (dest === ga) {
                console.log('[SIDECAR] delivered ga=' + ga + ' val=' + val +
                    ' after ' + attempts + ' attempt(s)');
                cleanup();
            }
        }
        // Secondary success signal: a bus telegram on the GA whose value matches
        // (for actuators that do echo their status on the same group address).
        function onEvt(apci, src, data) {
            if (done) return;
            var got = (data && data.length) ? (data[0] & 0x01) : null;
            if (got === desired) {
                console.log('[SIDECAR] confirmed ga=' + ga + ' val=' + val +
                    ' after ' + attempts + ' attempt(s)');
                cleanup();
            }
        }
        pending[ga] = cleanup;
        subCon = conn.on('confirmed', onConfirmed);
        sub = conn.on(evtName, onEvt);

        function attempt() {
            if (done) return;
            if (attempts >= MAX) {
                console.error('[SIDECAR] write NOT confirmed ga=' + ga + ' val=' +
                    val + ' after ' + MAX + ' attempts');
                cleanup();
                return;
            }
            attempts++;
            var c = bridge.knxConnection && bridge.knxConnection.connection;
            if (c) {
                try { c.write(ga, val, dpt); }
                catch (e) { console.error('[SIDECAR] write error:', e.message); }
            }
            timer = setTimeout(attempt, GAP);
        }
        attempt();
    }

    child.on('exit', function (code) {
        console.log('[SIDECAR] child exited code=' + code + ' - restarting in 5s');
        setTimeout(function () { startSetSidecar(bridge); }, 5000);
    });

    console.log('[SIDECAR] started pid=' + child.pid);

    // De-duplicate outbound tunnel writes.
    //
    // A single /set command is written to the bus TWICE: once by this sidecar
    // (raw conn.write, bypassing knx_connection's SEND_GAP queue) and once by
    // homie-sdk's own transport.set path ~400 ms later. Well-behaved routers
    // tolerate the redundant back-to-back TUNNELING_REQUESTs, but the zavod
    // (192.168.0.227:3699) and new Dom (192.168.0.88) routers do NOT: they ACK
    // the first request, ignore the second, and once that second request goes
    // unacknowledged knx.js retransmits it 3x and tears the tunnel down. The
    // sequence counter then desyncs and every following write fails until a
    // clean reconnect — the classic "relay switches every other time".
    //
    // Collapsing duplicate (ga,value) writes that land within DEDUP_MS to a
    // single tunnel request removes the poisoned second datagram. The window is
    // kept below KNX_WRITE_RETRY_MS so the deliberate confirmation retries above
    // still get through.
    var DEDUP_MS = parseInt(process.env.KNX_WRITE_DEDUP_MS, 10) || 1000;
    var writeWrapInstalled = false;
    function installWriteDedupe(conn) {
        if (writeWrapInstalled || !conn || typeof conn.write !== 'function' || conn.__writeDeduped) return;
        var origWrite = conn.write.bind(conn);
        var last = {}; // ga -> { val: <string>, ts: <ms> }
        conn.write = function (ga, val, dpt) {
            var now = Date.now();
            var key = String(val);
            var prev = last[ga];
            if (prev && prev.val === key && (now - prev.ts) < DEDUP_MS) {
                console.log('[SIDECAR] dedupe drop ga=' + ga + ' val=' + val);
                return;
            }
            last[ga] = { val: key, ts: now };
            return origWrite(ga, val, dpt);
        };
        conn.__writeDeduped = true;
        writeWrapInstalled = true;
        console.log('[SIDECAR] write dedupe installed (window=' + DEDUP_MS + 'ms)');
    }

    // FSM watchdog: recover the tunnelling state machine when it gets wedged
    // waiting for a TUNNELING_ACK. Some KNX/IP routers (notably the one at the
    // "zavod" site on the non-standard :3699 port) silently drop tunnel ACKs
    // and desync the outbound sequence counter, leaving every subsequent write
    // unacknowledged.
    //
    // The old behaviour reset the FSM to 'idle' after only 2 s. That was WRONG
    // for two reasons:
    //   1. It fired *before* knx.js's own ACK retry/reconnect cycle (~4 s), so
    //      the built-in recovery never ran.
    //   2. 'idle' keeps the same (now desynced) channel + seqnum alive, so the
    //      next write fails too — the relay only switches "every other time".
    //
    // Now the threshold sits ABOVE the built-in cycle (default 8 s, tunable via
    // KNX_FSM_STUCK_MS) so knx.js recovers on its own in the normal case, and
    // when the watchdog does fire as a last resort it forces a full reconnect
    // ('connecting' → DISCONNECT_REQUEST + CONNECT_REQUEST) which releases the
    // stale channel on the router and resets seqnum (see connected._onEnter),
    // so writes recover deterministically instead of staying broken.
    var STUCK_MS = parseInt(process.env.KNX_FSM_STUCK_MS, 10) || 8000;
    var stuckSince = null;
    setInterval(function () {
        var conn = bridge.knxConnection && bridge.knxConnection.connection;
        if (!conn) return;
        installWriteDedupe(conn);

        if (conn.state === 'sendDatagram' || conn.state === 'sendTunnReq_waitACK') {
            if (!stuckSince) {
                stuckSince = Date.now();
            } else if (Date.now() - stuckSince > STUCK_MS) {
                console.log('[WATCHDOG] FSM stuck in ' + conn.state + ' for >' +
                    Math.round(STUCK_MS / 1000) + 's, forcing tunnel reconnect');
                if (conn.tunnelingAckTimer) {
                    clearTimeout(conn.tunnelingAckTimer);
                    conn.tunnelingAckTimer = null;
                }
                conn.numberOfWaitingACKFORTunnellingReqAttempts = 0;
                conn.transition('connecting');
                stuckSince = null;
            }
        } else {
            stuckSince = null;
        }
    }, 500);
}

module.exports = { startSetSidecar: startSetSidecar };
