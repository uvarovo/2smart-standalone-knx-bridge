#!/usr/bin/env node
// Post-install patch for node_modules/knx/src/FSM.js
// Adds ACK handling in sendDatagram and sendTunnReq_waitACK states
// to prevent tunnel drops when the gateway sends inbound frames while
// the bridge is waiting for an outgoing ACK.
'use strict';

const fs   = require('fs');
const path = require('path');

const fsmPath = path.join(__dirname, '..', 'node_modules', 'knx', 'src', 'FSM.js');

if (!fs.existsSync(fsmPath)) {
    console.log('[patch-knx-fsm] knx/src/FSM.js not found, skipping');
    process.exit(0);
}

let fsm = fs.readFileSync(fsmPath, 'utf8');

const states = ['sendDatagram', 'sendTunnReq_waitACK'];
let patched  = false;

const handler = `
      // [PATCH] ACK inbound frames even while sending (prevents tunnel drop)
      'inbound_TUNNELING_REQUEST_L_Data.ind': function(datagram) {
        if (this.useTunneling) {
          var sm = this;
          sm.send(sm.prepareDatagram(KnxConstants.SERVICE_TYPE.TUNNELING_ACK, datagram), function(){});
          sm.emit('event', { src: datagram.cemi.src_addr, dest: datagram.cemi.dest_addr, apdu: datagram.cemi.apdu });
        }
      },
`;

states.forEach(function (stateName) {
    const patterns = [
        stateName + ':  {',
        stateName + ': {',
        stateName + ':{'
    ];
    let pos = -1;

    for (const p of patterns) {
        pos = fsm.indexOf(p);
        if (pos >= 0) break;
    }
    if (pos < 0) { console.log('[patch-knx-fsm] state ' + stateName + ' not found'); return; }

    const bracePos = fsm.indexOf('{', pos);
    const chunk    = fsm.substring(bracePos, bracePos + 300);

    if (chunk.includes('inbound_TUNNELING_REQUEST')) {
        console.log('[patch-knx-fsm] ' + stateName + ' already patched');
        return;
    }

    fsm = fsm.substring(0, bracePos + 1) + handler + fsm.substring(bracePos + 1);
    patched = true;
    console.log('[patch-knx-fsm] patched: ' + stateName);
});

if (patched) {
    fs.writeFileSync(fsmPath, fsm);
    console.log('[patch-knx-fsm] FSM.js updated');
} else {
    console.log('[patch-knx-fsm] nothing to patch');
}
