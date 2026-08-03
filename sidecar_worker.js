// Sidecar worker - runs in a child process with its own MQTT client.
// Handles /set commands and publishes the full device tree to MQTT.
// This bypasses the EMQX 4.x delivery stall that occurs when a bridge
// has many mappings (>200) and the main homie-sdk MQTT client receives
// a large burst of retained messages at startup.
'use strict';

var mqtt = require('mqtt');
var fs   = require('fs');

var deviceId = process.env.DEVICE_ID || process.env.MQTT_USER;
var mqttUri  = process.env.MQTT_URI || 'mqtt://2smart-emqx:1883';
var mqttUser = process.env.MQTT_USER || '2smart';
var mqttPass = process.env.MQTT_PASS || '';

var topics = [
    'sweet-home/' + deviceId + '/+/+/set',
    'sweet-home/' + deviceId + '/+/set',
    'sweet-home/' + deviceId + '/$heartbeat/set'
];

// Set of homie value topics (sweet-home/<dev>/<node>/<prop>) whose datatype is
// boolean, and the retained values we observe for them at startup. Used to
// (a) normalize every boolean state publish to the strict homie "true"/"false"
// wire format and (b) repair the retained state already sitting in the broker.
var boolTopics    = {}; // value topic -> true
var retainedBool  = {}; // value topic -> last retained payload seen at startup

// Coerce any truthy/falsy KNX/homie representation to the strict homie boolean
// wire value. homie's boolean datatype accepts ONLY "true"/"false"; anything
// else (the numeric "1"/"0" a KNX DPT1 echo produces, "on"/"off", empty) is
// invalid and the Dashboard silently drops the property — which is exactly why
// relays with a "1"/"0" retained value disappear from the UI.
function toHomieBool(v) {
    v = (v === undefined || v === null) ? '' : String(v).trim().toLowerCase();
    return (v === '1' || v === 'true' || v === 'on' || v === 'yes') ? 'true' : 'false';
}

// Populate boolTopics from the node config. Must run BEFORE we subscribe to the
// value topics, otherwise the retained boolean values delivered right after the
// SUBACK would be ignored (boolTopics still empty) and the repair pass would
// treat every relay as "missing" and seed it to false, wiping real state.
// Uses the same DPT->datatype derivation as publishDeviceTree().
function buildBoolTopics() {
    var config;
    try {
        config = JSON.parse(fs.readFileSync('/etc/nodes.config.json', 'utf8'));
    } catch (e) {
        console.error('[SIDECAR-W] buildBoolTopics config read error:', e.message);
        return;
    }
    var nodes   = (config.deviceConfig && config.deviceConfig.nodes) || [];
    var mapping = (config.extensions && config.extensions.mapping) || {};
    var prefix  = 'sweet-home/' + deviceId;

    nodes.forEach(function (node) {
        (node.sensors || []).forEach(function (s) {
            var m        = mapping[node.id + '/' + s.id];
            var dpt      = (m && m.transport && m.transport.dpt) || '';
            var datatype = s.dataType;
            if (!datatype) {
                if (dpt.indexOf('DPT1') === 0) datatype = 'boolean';
                else if (dpt.indexOf('DPT9') === 0 || dpt.indexOf('DPT14') === 0) datatype = 'float';
                else if (dpt.indexOf('DPT5') === 0 || dpt.indexOf('DPT7') === 0) datatype = 'integer';
            }
            if (datatype === 'boolean') boolTopics[prefix + '/' + node.id + '/' + s.id] = true;
        });
    });
    console.error('[SIDECAR-W] boolean props tracked: ' + Object.keys(boolTopics).length);
}

var client = mqtt.connect(mqttUri, {
    username  : mqttUser,
    password  : mqttPass,
    clientId  : 'scw_' + deviceId.substr(0, 8) + '_' + Date.now().toString(36),
    clean     : true,
    keepalive : 60
});

// Publish the full device tree (nodes, properties, datatypes) from config.
// This ensures the Dashboard shows the device even if the main bridge
// process hasn't completed initWorld() yet.
function publishDeviceTree() {
    var opts   = { retain: true, qos: 1 };
    var prefix = 'sweet-home/' + deviceId;
    var deviceName = process.env.DEVICE_NAME || 'KNX Bridge';

    // Device-level attributes required by homie-sdk validateStructure()
    client.publish(prefix + '/$homie', '3.0.1', opts);
    client.publish(prefix + '/$name', deviceName, opts);
    client.publish(prefix + '/$localip', '-', opts);
    client.publish(prefix + '/$mac', '-', opts);
    client.publish(prefix + '/$implementation', 'Bridge', opts);
    client.publish(prefix + '/$state', 'ready', opts);
    client.publish(prefix + '/$fw/name', '2smart-knx-bridge', opts);
    client.publish(prefix + '/$fw/version', process.env.DEVICE_FIRMWARE_VERSION || 'market', opts);

    // Load node configuration
    var config;
    try {
        config = JSON.parse(fs.readFileSync('/etc/nodes.config.json', 'utf8'));
    } catch (e) {
        console.error('[SIDECAR-W] cannot read config:', e.message);
        return;
    }

    var nodes   = config.deviceConfig && config.deviceConfig.nodes;
    var mapping = (config.extensions && config.extensions.mapping) || {};
    if (!nodes || !nodes.length) return;

    // $nodes — comma-separated list of node IDs
    var nodeIds = nodes.map(function (n) { return n.id; });
    client.publish(prefix + '/$nodes', nodeIds.join(','), opts);

    // Build all messages for node metadata
    var messages = [];
    nodes.forEach(function (node) {
        var np = prefix + '/' + node.id;
        messages.push([np + '/$name', node.name || node.id]);
        messages.push([np + '/$type', node.type || 'knx']);

        var sensors = node.sensors || [];
        var propIds = sensors.map(function (s) { return s.id; });
        messages.push([np + '/$properties', propIds.join(',')]);

        sensors.forEach(function (s) {
            var sp = np + '/' + s.id;
            messages.push([sp + '/$name', s.name || s.id]);
            messages.push([sp + '/$settable', s.settable ? 'true' : 'false']);
            messages.push([sp + '/$retained', s.retained !== false ? 'true' : 'false']);

            // Derive datatype from DPT in transport mapping
            var mapKey  = node.id + '/' + s.id;
            var m       = mapping[mapKey];
            var dpt     = (m && m.transport && m.transport.dpt) || '';
            var datatype = 'string';
            var unit     = '';

            if (dpt.indexOf('DPT9') === 0 || dpt.indexOf('DPT14') === 0) {
                datatype = 'float'; unit = '\u00B0C';
            } else if (dpt.indexOf('DPT5') === 0 || dpt.indexOf('DPT7') === 0) {
                datatype = 'integer';
            } else if (dpt.indexOf('DPT1') === 0) {
                datatype = 'boolean';
            }

            if (s.dataType) datatype = s.dataType;
            if (s.unit) unit = s.unit;

            messages.push([sp + '/$datatype', datatype]);
            if (unit) messages.push([sp + '/$unit', unit]);

            if (datatype === 'boolean') boolTopics[sp] = true;
        });
    });

    // Throttled publish: 20 messages per 100 ms to avoid socket overflow
    var BATCH = 20;
    var idx   = 0;

    function sendBatch() {
        var end = Math.min(idx + BATCH, messages.length);
        for (var i = idx; i < end; i++) {
            client.publish(messages[i][0], messages[i][1], opts);
        }
        idx = end;
        if (idx < messages.length) {
            setTimeout(sendBatch, 100);
        } else {
            console.error('[SIDECAR-W] published device tree: ' + messages.length + ' messages');
        }
    }
    sendBatch();
}

// Repair the retained boolean state already sitting in the broker.
//
// Historically relay state reached MQTT through several code paths that
// published the raw KNX DPT1 value ("1"/"0") rather than the strict homie
// "true"/"false", and relays that were never toggled since the device was
// provisioned have no value topic at all. Both are invalid for a homie
// "boolean" property, so the Dashboard drops them and only a subset of the
// relays shows up after a page reload — the classic "14 relays, only 5-10
// visible". New commands now publish the correct format, but they never touch
// the stale/absent retained values, which survive container restarts.
//
// So, once, after the tree is published and we have had a moment to observe the
// existing retained values (collected in retainedBool via the message handler),
// rewrite every boolean value topic to a valid homie boolean: "1"/"0" become
// "true"/"false" (state preserved), and a missing value is seeded to "false"
// (unknown — actuators on these routers do not report their state, so a safe
// default that at least makes the relay appear and be operable). Topics that
// are already "true"/"false" are left untouched, so a fresh command that landed
// during the observation window is never clobbered.
var stateRepaired = false;
function repairBooleanState() {
    if (stateRepaired) return;
    stateRepaired = true;

    var opts = { retain: true, qos: 1 };
    var fixed = 0;
    var seeded = 0;

    Object.keys(boolTopics).forEach(function (topic) {
        var cur = retainedBool[topic];
        if (cur === 'true' || cur === 'false') return; // already valid
        var next = toHomieBool(cur);
        if (cur === undefined) seeded++; else fixed++;
        client.publish(topic, next, opts);
    });

    if (fixed || seeded) {
        console.error('[SIDECAR-W] repaired boolean state: normalized=' + fixed +
            ' seeded=' + seeded + ' (of ' + Object.keys(boolTopics).length + ' boolean props)');
    }
}

client.on('connect', function () {
    console.error('[SIDECAR-W] connected');
    client.subscribe(topics, function (err) {
        if (err) console.error('[SIDECAR-W] subscribe error:', err.message);
        else console.error('[SIDECAR-W] subscribed');
    });

    // Know which value topics are boolean before observing retained values.
    buildBoolTopics();

    // Observe the boolean value topics already retained in the broker so
    // repairBooleanState() can normalize/seed them below. Metadata topics
    // (.../$name etc.) have an extra segment and do not match this filter.
    client.subscribe('sweet-home/' + deviceId + '/+/+', { qos: 1 }, function (err) {
        if (err) console.error('[SIDECAR-W] value subscribe error:', err.message);
    });

    // Publish device tree after a short delay (let connection stabilize)
    setTimeout(publishDeviceTree, 2000);

    // Repair retained boolean state after the tree is up and we have had time
    // to receive the currently retained values.
    setTimeout(repairBooleanState, 6000);

    // Keep $state=ready alive every 30 s — the main process LWT publishes
    // $state=disconnected on any brief reconnect; this overrides it.
    setInterval(function () {
        client.publish('sweet-home/' + deviceId + '/$state', 'ready', { retain: true, qos: 1 });
    }, 30000);
});

client.on('message', function (topic, msg, packet) {
    var payload = msg.toString();

    // Observe currently-retained boolean values (from the +/+ subscription) so
    // repairBooleanState() can normalize/seed them. Only retained deliveries of
    // an actual boolean value topic are relevant.
    if (packet && packet.retain && !/\/set$/.test(topic) && boolTopics[topic]) {
        retainedBool[topic] = payload;
        return;
    }

    // Publish the homie state directly and immediately from THIS client, which
    // reliably received the command. The main homie process's own client is
    // flaky (it intermittently misses commands and delivers its state publishes
    // seconds late/out of order, reverting the UI), so this worker is the sole
    // authoritative state publisher. Doing it right here — instead of via a
    // round trip through the parent — keeps state reflection exactly as reliable
    // as command reception. Retained + qos1 so a page refresh shows the value.
    if (/\/set$/.test(topic)) {
        var stateTopic = topic.replace(/\/set$/, '');
        // Boolean properties must be published in the strict homie "true"/"false"
        // wire format; a "1"/"0" (or other truthy/falsy) command would otherwise
        // be republished verbatim and make the relay vanish from the Dashboard.
        var stateValue = boolTopics[stateTopic] ? toHomieBool(payload) : payload;
        client.publish(stateTopic, stateValue, { retain: true, qos: 1 }, function (e) {
            if (e) console.error('[SIDECAR-W] state publish error: ' + e.message);
        });
        // Reflect the freshly commanded value so a repair pass never overwrites it.
        if (boolTopics[stateTopic]) retainedBool[stateTopic] = stateValue;
    }
    // Forward ONLY commands to the parent (app_set_sidecar.js), which drives the
    // KNX bus write. Messages from the '+/+' value-topic subscription above must
    // never be forwarded: the parent maps <node>/<prop> to a group address and
    // writes it, so a plain state publish would be echoed straight back onto the
    // bus as a GroupValue_Write. Live deliveries carry retain=false, so they slip
    // past the retained-observation branch above — meaning every sensor refresh
    // and every repairBooleanState() publish used to write to the bus.
    if (!/\/set$/.test(topic)) return;
    process.send({ topic: topic, payload: payload });
});

client.on('error', function (err) {
    console.error('[SIDECAR-W] MQTT error:', err.message);
});
