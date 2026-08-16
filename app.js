require('events').defaultMaxListeners = 100;
const dgram = require('dgram');
const fs = require('fs');
const Debugger = require('homie-sdk/lib/utils/debugger');
const KnxBridge = require('./lib/knx-bridge/bridge');
// const KnxLog = require('./lib/knx/src/KnxLog');

const debug = new Debugger(process.env.DEBUG || '');

debug.initEvents();
// KnxLog.get().initEvents();
try {
    // NODES
    let nodes = null;

    const config = require('./etc/nodes.config.json');

    nodes = config.deviceConfig.nodes;

    const mapping = (config.extensions && config.extensions.mapping) || {};
    const mappingPing = (config.extensions && config.extensions.mappingPing) || {};

    // eslint-disable-next-line guard-for-in
    for (const key in mapping) {
        const keys = key.split('/');

        const node = nodes.find((n) => {
            return n.id === keys[0];
        });

        if (!node) throw new Error(`Cannot find node with id ${keys[0]}`);
        let property = null;

        if (keys[1] === '$options') {
            property = node.options.find((p) => p.id === keys[2]);
            if (!property) throw new Error(`Cannot find option with id ${keys[2]} in node ${keys[0]}`);
        } else if (keys[1] === '$telemetry') {
            property = node.telemetry.find((p) => p.id === keys[2]);
            if (!property) throw new Error(`Cannot find telemetry with id ${keys[2]} in node ${keys[0]}`);
        } else {
            property = node.sensors.find((p) => p.id === keys[1]);
            if (!property) throw new Error(`Cannot find sensor with id ${keys[1]} in node ${keys[0]}`);
        }
        Object.assign(property, mapping[key]);
    }
    // eslint-disable-next-line guard-for-in
    for (const key in mappingPing) {
        const node = nodes.find((n) => {
            return n.id === key;
        });

        if (!node) throw new Error(`Cannot find node with id ${key} to map ping`);
        node.ping = mappingPing[key];
    }
    // NODES END

    if (process.env.KNX_CONNECTION_FORCE_TUNNELING && (process.env.KNX_CONNECTION_FORCE_TUNNELING !== 'true' || process.env.KNX_CONNECTION_FORCE_TUNNELING !== 'false')) {
        throw new Error('Environment variable KNX_CONNECTION_FORCE_TUNNELING must be either true or false.');
    }

    // Source UDP-port binding for the KNX/IP tunnel.
    // Some KNX/IP gateways (notably DIY/self-built ones) keep stale tunnel
    // state keyed by the client's (source IP, source port) tuple. If the
    // bridge ever crashes or the gateway is rebooted while the bridge keeps
    // sending CONNECT_REQUEST from the same source port, the gateway can
    // refuse new connections forever ("client already connected"). Letting
    // the OS pick a fresh ephemeral port on every restart sidesteps that:
    // the gateway sees a brand-new client and accepts the tunnel.
    //
    // Accepted values:
    //   "auto" | unset | "0"        -> OS picks ephemeral port (recommended)
    //   "<port>"                    -> fixed port, both receive and listen
    //   "<receive>:<listen>"        -> separate receive/listen ports
    const portBindingRaw = (process.env.KNX_CONNECTION_LOCAL_PORT_BINDING || 'auto').trim();
    const ports = (portBindingRaw === 'auto' || portBindingRaw === '0' || portBindingRaw === '')
        ? [ 0, 0 ]
        : portBindingRaw.split(':').map((p) => parseInt(p, 10));

    if (ports.length === 1) ports[1] = ports[0];
    if (ports.some((p) => Number.isNaN(p) || p < 0 || p > 65535)) {
        throw new Error(`Invalid KNX_CONNECTION_LOCAL_PORT_BINDING="${portBindingRaw}". Use "auto", "<port>", or "<receive>:<listen>".`);
    }

    const deviceBridgeConfig = {
        mqttConnection : {
            username : process.env.MQTT_USER || undefined,
            password : process.env.MQTT_PASS || undefined,
            uri      : process.env.MQTT_URI || undefined
        },
        knxConnection : {
            ipAddr         : process.env.KNX_CONNECTION_IP_ADDR || undefined,
            ipPort         : parseInt(process.env.KNX_CONNECTION_IP_PORT, 10) || 3671,
            physAddr       : process.env.KNX_CONNECTION_PHYS_ADDR || undefined,
            forceTunneling : JSON.parse(process.env.KNX_CONNECTION_FORCE_TUNNELING || 'true'),
            localIp        : process.env.KNX_CONNECTION_LOCAL_IP || undefined,
            receivePort    : ports[0],
            listenPort     : ports[1]
        },
        device : {
            id              : process.env.DEVICE_ID || process.env.MQTT_USER || undefined,
            name            : process.env.DEVICE_NAME || undefined,
            implementation  : process.env.DEVICE_IMPLEMENTATION || undefined,
            mac             : process.env.DEVICE_MAC || undefined,
            firmwareVersion : process.env.DEVICE_FIRMWARE_VERSION || undefined,
            firmwareName    : process.env.DEVICE_FIRMWARE_NAME || undefined,
            nodes
        }
    };

    // eslint-disable-next-line no-inner-declarations
    const knxBridge = new KnxBridge({ ...deviceBridgeConfig, debug });

    // Many KNX group addresses are write-only (sensors push GroupValue_Write,
    // they don't respond to GroupValue_Read). Polling such GAs always times
    // out with "No response(GroupValue_Response_X/Y/Z) timeout." — that's
    // expected, not a real error. Suppress those from the error log so it
    // doesn't get flooded; everything else still logs as before.
    const isReadTimeout = (err) => err && typeof err.message === 'string'
        && /No response\(GroupValue_Response_[\d/]+\) timeout\./.test(err.message);
    knxBridge.on('error', (error) => {
        if (isReadTimeout(error)) return;
        debug.error(error);
    });
    knxBridge.on('exit', (reason, exit_code) => {
        debug.error(reason);
        process.exit(exit_code);
    });

    // Watchdog: some KNX/IP gateways (notably DIY/self-built ones) keep stale
    // tunnel state for our source endpoint and silently ignore CONNECT_REQUEST
    // until that state expires. The FSM in vendored knx.js never closes/rebinds
    // the UDP socket, so retries from the same socket keep the gateway stuck.
    // If we're disconnected (or never connected) for too long, exit so Docker
    // restarts the container with a fresh socket and ephemeral source port —
    // gateway sees a new client and accepts the connection.
    //
    // A restart only helps while the gateway is actually reachable. When it is
    // offline (powered down, unplugged, IP changed) exiting every few minutes
    // becomes an endless container restart loop that loads the host and eats
    // the gateway's tunnel slots once it comes back. So probe the gateway first
    // and only restart when it answers; while it stays silent, idle in place and
    // let the FSM keep retrying.
    const WATCHDOG_TIMEOUT_MS = parseInt(process.env.KNX_WATCHDOG_TIMEOUT_MS, 10) || 60000;
    // Safety valve for gateways that never answer DESCRIPTION_REQUEST: they'd
    // look permanently offline, so still allow an occasional socket refresh.
    const WATCHDOG_OFFLINE_RESTART_MS = parseInt(process.env.KNX_WATCHDOG_OFFLINE_RESTART_MS, 10) || 1800000;
    const OFFLINE_LOG_INTERVAL_MS = 300000;
    // A gateway can answer DESCRIPTION_REQUEST and still refuse tunnels (all
    // slots busy), where restarting fixes nothing. Widen the window after a few
    // fruitless attempts so a short timeout cannot become its own restart loop.
    const WATCHDOG_BACKOFF_AFTER = parseInt(process.env.KNX_WATCHDOG_BACKOFF_AFTER, 10) || 3;
    const WATCHDOG_BACKOFF_TIMEOUT_MS = parseInt(process.env.KNX_WATCHDOG_BACKOFF_TIMEOUT_MS, 10) || 900000;
    const ATTEMPTS_FILE = process.env.KNX_WATCHDOG_STATE_FILE || '/tmp/knx-watchdog-attempts.json';
    const ATTEMPTS_TTL_MS = 3600000;
    const STABLE_AFTER_MS = 120000;

    const watchdogState = { offlineSince: null, loggedAt: 0, busy: false, cleared: false, exiting: false };

    // Fruitless restart attempts outlive the process exit in a small state file,
    // so a restarted container knows whether the previous restarts helped.
    let attempts = 0;

    fs.readFile(ATTEMPTS_FILE, 'utf8', (err, raw) => {
        if (err) return;
        try {
            const saved = JSON.parse(raw);

            if (Date.now() - saved.at <= ATTEMPTS_TTL_MS) attempts = saved.count || 0;
        } catch (e) {
            // malformed state file, start from scratch
        }
    });

    const clearAttempts = () => {
        attempts = 0;
        fs.unlink(ATTEMPTS_FILE, () => {
            // nothing to clear
        });
    };
    const exitAfterAttempt = () => {
        watchdogState.exiting = true;
        const saved = JSON.stringify({ count: attempts + 1, at: Date.now() });

        fs.writeFile(ATTEMPTS_FILE, saved, () => process.exit(1));
    };

    let knxConnected = false;
    let lastConnectionEventAt = Date.now();

    // Resolves true when the gateway answers a KNXnet/IP DESCRIPTION_REQUEST.
    const probeGateway = (ipAddr, ipPort, timeoutMs) => new Promise((resolve) => {
        const socket = dgram.createSocket('udp4');

        let settled = false;
        const finish = (alive) => {
            if (settled) return;
            settled = true;
            clearTimeout(timer);
            try {
                socket.close();
            } catch (e) {
                // socket already closed
            }
            resolve(alive);
        };
        const timer = setTimeout(() => finish(false), timeoutMs);
        // header (0x0203 DESCRIPTION_REQUEST, 14 bytes) + unused HPAI 0.0.0.0:0
        const request = Buffer.from([ 0x06, 0x10, 0x02, 0x03, 0x00, 0x0e, 0x08, 0x01, 0, 0, 0, 0, 0, 0 ]);

        socket.on('message', () => finish(true));
        socket.on('error', () => finish(false));
        socket.send(request, ipPort, ipAddr, (err) => {
            if (err) finish(false);
        });
    });

    knxBridge.on('knx.connected', () => {
        knxConnected = true;
        lastConnectionEventAt = Date.now();
    });
    knxBridge.on('knx.disconnected', () => {
        knxConnected = false;
        lastConnectionEventAt = Date.now();
    });

    setInterval(async () => {
        if (watchdogState.exiting) return;
        if (knxConnected) {
            watchdogState.offlineSince = null;
            watchdogState.loggedAt = 0;
            // Only a connection that holds counts as a successful recovery.
            if (!watchdogState.cleared && Date.now() - lastConnectionEventAt >= STABLE_AFTER_MS) {
                watchdogState.cleared = true;
                clearAttempts();
            }

            return;
        }
        watchdogState.cleared = false;
        if (watchdogState.busy) return;
        const stuckFor = Date.now() - lastConnectionEventAt;
        const timeout = attempts >= WATCHDOG_BACKOFF_AFTER ? WATCHDOG_BACKOFF_TIMEOUT_MS : WATCHDOG_TIMEOUT_MS;

        if (stuckFor < timeout) return;

        watchdogState.busy = true;
        try {
            const { ipAddr, ipPort } = deviceBridgeConfig.knxConnection;
            const gateway = `${ipAddr}:${ipPort}`;
            const alive = ipAddr ? await probeGateway(ipAddr, ipPort, 4000) : true;

            if (alive) {
                debug.error(`KNX watchdog: not connected for ${Math.round(stuckFor / 1000)}s while gateway ${gateway} answers — exiting so Docker restarts the container with a fresh UDP socket.`);
                console.error(new Date(), `KNX watchdog: stuck disconnected for ${Math.round(stuckFor / 1000)}s, attempt ${attempts + 1}, exiting.`);
                exitAfterAttempt();

                return;
            }

            if (!watchdogState.offlineSince) watchdogState.offlineSince = Date.now();
            const offlineFor = Date.now() - watchdogState.offlineSince;

            if (Date.now() - watchdogState.loggedAt >= OFFLINE_LOG_INTERVAL_MS) {
                watchdogState.loggedAt = Date.now();
                console.error(new Date(), `KNX watchdog: gateway ${gateway} unreachable for ${Math.round(offlineFor / 1000)}s — staying up and retrying instead of restarting.`);
            }

            if (offlineFor >= WATCHDOG_OFFLINE_RESTART_MS) {
                console.error(new Date(), `KNX watchdog: gateway ${gateway} silent for ${Math.round(offlineFor / 1000)}s, exiting once to refresh the UDP socket.`);
                process.exit(1);
            }
        } finally {
            watchdogState.busy = false;
        }
    }, 15000).unref();

    knxBridge.init();

    // Auto-start /set sidecar — spawns a child process with an independent
    // MQTT client that handles /set commands directly.  This bypasses the
    // EMQX 4.x delivery stall that affects the main homie-sdk client when
    // the bridge has many mappings (>200 retained messages at startup).
    try {
        const { startSetSidecar } = require('./app_set_sidecar');
        startSetSidecar(knxBridge);
    } catch (sidecarErr) {
        console.error('[SIDECAR] startup failed:', sidecarErr.message);
    }
} catch (e) {
    debug.error(e);
    process.exit(1);
}
