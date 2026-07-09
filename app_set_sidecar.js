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
        try {
            conn.write(ga, val, dpt);
        } catch (e) {
            console.error('[SIDECAR] write error:', e.message);
        }
    });

    child.on('exit', function (code) {
        console.log('[SIDECAR] child exited code=' + code + ' - restarting in 5s');
        setTimeout(function () { startSetSidecar(bridge); }, 5000);
    });

    console.log('[SIDECAR] started pid=' + child.pid);

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
