#!/usr/bin/env node
// Post-install patch for node_modules/homie-sdk/lib/Bridge/Property/index.js
//
// Normalizes boolean-datatype property values to the canonical homie tokens
// "true"/"false" at the single publish/seed choke point of BasePropertyBridge.
//
// Why: after the EMQX v4->v5 broker migration some retained boolean states
// carried over in the old raw form ("0"/"1"/"undefined") instead of homie
// "true"/"false". homie-sdk seeds `lastValue` from that retained value at
// startup (constructor) and then RE-ASSERTS it on every publish
// (handlePublish), so a raw "0" is actively held in memory and overwrites any
// valid value within ~10ms. A one-time retained cleanup therefore cannot hold
// on a live bridge. Coercing at publishAttribute + the constructor seed makes
// the raw form impossible to seed or re-assert, so the state is always valid
// homie boolean. Already-valid "true"/"false" pass through unchanged, so this
// is a no-op for healthy properties.
'use strict';

const fs   = require('fs');
const path = require('path');

const file = path.join(__dirname, '..', 'node_modules', 'homie-sdk', 'lib', 'Bridge', 'Property', 'index.js');

if (!fs.existsSync(file)) {
    console.log('[patch-homie-boolean] Property/index.js not found, skipping');
    process.exit(0);
}

let src = fs.readFileSync(file, 'utf8');

if (src.indexOf('__canonHomieBool') >= 0) {
    console.log('[patch-homie-boolean] already patched');
    process.exit(0);
}

const helper = `
// [PATCH] canonical homie boolean token (see patches/patch-homie-boolean.js)
function __canonHomieBool(value) {
    if (value === true  || value === 'true'  || value === 1 || value === '1') return 'true';
    if (value === false || value === 'false' || value === 0 || value === '0') return 'false';
    if (value === 'undefined') return 'false';
    return value;
}
`;

const seedFrom = 'if (homieProperty.value) this.lastValue = homieProperty.value;';
const seedTo   = "if (homieProperty.value) this.lastValue = (parser && parser.homieDataType === 'boolean') ? __canonHomieBool(homieProperty.value) : homieProperty.value;";

const pubFrom = `    publishAttribute(key, value, forced) {
        if (key === 'value') this.lastValue = value;`;
const pubTo = `    publishAttribute(key, value, forced) {
        if (key === 'value') {
            const dt = (this.parser && this.parser.homieDataType) || (this.getAttribute ? this.getAttribute('dataType') : undefined);
            if (dt === 'boolean') value = __canonHomieBool(value);
        }
        if (key === 'value') this.lastValue = value;`;

let ok = true;

if (src.indexOf(seedFrom) >= 0) {
    src = src.replace(seedFrom, seedTo);
    console.log('[patch-homie-boolean] patched constructor seed');
} else {
    console.log('[patch-homie-boolean] seed anchor NOT found'); ok = false;
}

if (src.indexOf(pubFrom) >= 0) {
    src = src.replace(pubFrom, pubTo);
    console.log('[patch-homie-boolean] patched publishAttribute');
} else {
    console.log('[patch-homie-boolean] publishAttribute anchor NOT found'); ok = false;
}

// Insert helper after the module's require block (before class declaration).
const classAnchor = 'class BasePropertyBridge extends BaseEntityBridge {';
if (src.indexOf(classAnchor) >= 0) {
    src = src.replace(classAnchor, helper + '\n' + classAnchor);
} else {
    console.log('[patch-homie-boolean] class anchor NOT found'); ok = false;
}

if (ok) {
    fs.writeFileSync(file, src);
    console.log('[patch-homie-boolean] Property/index.js updated');
} else {
    console.error('[patch-homie-boolean] one or more anchors missing - NOT writing (homie-sdk layout changed?)');
    process.exit(1);
}
