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

client.on('connect', function () {
    console.error('[SIDECAR-W] connected');
    client.subscribe(topics, function (err) {
        if (err) console.error('[SIDECAR-W] subscribe error:', err.message);
        else console.error('[SIDECAR-W] subscribed');
    });

    // Publish device tree after a short delay (let connection stabilize)
    setTimeout(publishDeviceTree, 2000);

    // Keep $state=ready alive every 30 s — the main process LWT publishes
    // $state=disconnected on any brief reconnect; this overrides it.
    setInterval(function () {
        client.publish('sweet-home/' + deviceId + '/$state', 'ready', { retain: true, qos: 1 });
    }, 30000);
});

client.on('message', function (topic, msg) {
    var payload = msg.toString();
    // Publish the homie state directly and immediately from THIS client, which
    // reliably received the command. The main homie process's own client is
    // flaky (it intermittently misses commands and delivers its state publishes
    // seconds late/out of order, reverting the UI), so this worker is the sole
    // authoritative state publisher. Doing it right here — instead of via a
    // round trip through the parent — keeps state reflection exactly as reliable
    // as command reception. Retained + qos1 so a page refresh shows the value.
    if (/\/set$/.test(topic)) {
        client.publish(topic.replace(/\/set$/, ''), payload, { retain: true, qos: 1 }, function (e) {
            if (e) console.error('[SIDECAR-W] state publish error: ' + e.message);
        });
    }
    // Forward to parent (app_set_sidecar.js) which drives the KNX bus write.
    process.send({ topic: topic, payload: payload });
});

client.on('error', function (err) {
    console.error('[SIDECAR-W] MQTT error:', err.message);
});
