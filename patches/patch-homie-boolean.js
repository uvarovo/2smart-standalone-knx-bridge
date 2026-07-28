#!/usr/bin/env node
// Post-install patch for node_modules/homie-sdk/lib/Bridge/Property/index.js
//
// Normalizes boolean-datatype property state to the canonical homie tokens
// "true"/"false" so the raw form ("0"/"1"/"undefined") left over from the
// EMQX v4->v5 broker migration can neither survive nor be re-asserted.
//
// Why three choke points:
//   1) constructor seed         - if a value is known at construction, coerce
//                                  the seeded `lastValue`.
//   2) handlePublish (ingest)   - the retained value actually arrives from the
//                                  broker AFTER the property is constructed, as
//                                  a publish on the value topic. This is the
//                                  point that heals a migrated raw "0": we
//                                  coerce the incoming value, remember it as
//                                  `lastValue`, and re-publish the canonical
//                                  form so the retained topic is rewritten.
//   3) publishAttribute (egest) - every value we publish is canonical, and
//                                  `lastValue` stays canonical so the sdk's
//                                  re-assert logic can only ever assert a
//                                  valid boolean.
//
// homie-sdk seeds `lastValue` from the retained value and re-asserts it on
// every publish, so on a live bridge a migrated raw "0" is held in memory and
// snaps any valid value back within ~10ms; a one-time retained cleanup cannot
// hold (observed on Dom Village ABB actuator outputs g/n). Coercing at these
// points makes the raw form impossible. Already-valid "true"/"false" pass
// through unchanged, so this is a no-op for healthy properties.
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
function __homieBoolDataType(self) {
    return (self.parser && self.parser.homieDataType) || (self.getAttribute ? self.getAttribute('dataType') : undefined);
}
`;

const anchors = [
    {
        name : 'constructor seed',
        from : 'if (homieProperty.value) this.lastValue = homieProperty.value;',
        to   : "if (homieProperty.value) this.lastValue = (parser && parser.homieDataType === 'boolean') ? __canonHomieBool(homieProperty.value) : homieProperty.value;"
    },
    {
        name : 'publishAttribute',
        from : `    publishAttribute(key, value, forced) {
        if (key === 'value') this.lastValue = value;`,
        to : `    publishAttribute(key, value, forced) {
        if (key === 'value' && __homieBoolDataType(this) === 'boolean') value = __canonHomieBool(value);
        if (key === 'value') this.lastValue = value;`
    },
    {
        name : 'handlePublish',
        from : `    async handlePublish(data) {
        const key = Object.keys(data)[0];

        await super.handlePublish(data);

        if (('lastValue' in this) && key === 'value' && !_isEqual(this.getAttribute(key), \`\${this.lastValue}\`)) {
            this.publishAttribute('value', this.lastValue);
        }
    }`,
        to : `    async handlePublish(data) {
        const key = Object.keys(data)[0];

        // [PATCH] heal migrated raw boolean state on ingestion
        if (key === 'value' && __homieBoolDataType(this) === 'boolean') {
            this.lastValue = __canonHomieBool(data[key]);

            await super.handlePublish(data);

            if (!_isEqual(this.getAttribute(key), \`\${this.lastValue}\`)) {
                this.publishAttribute('value', this.lastValue);
            }

            return;
        }

        await super.handlePublish(data);

        if (('lastValue' in this) && key === 'value' && !_isEqual(this.getAttribute(key), \`\${this.lastValue}\`)) {
            this.publishAttribute('value', this.lastValue);
        }
    }`
    }
];

const classAnchor = 'class BasePropertyBridge extends BaseEntityBridge {';
let ok = true;

for (const a of anchors) {
    if (src.indexOf(a.from) >= 0) {
        src = src.replace(a.from, a.to);
        console.log('[patch-homie-boolean] patched ' + a.name);
    } else {
        console.log('[patch-homie-boolean] anchor NOT found: ' + a.name);
        ok = false;
    }
}

if (src.indexOf(classAnchor) >= 0) {
    src = src.replace(classAnchor, helper + '\n' + classAnchor);
} else {
    console.log('[patch-homie-boolean] class anchor NOT found');
    ok = false;
}

if (ok) {
    fs.writeFileSync(file, src);
    console.log('[patch-homie-boolean] Property/index.js updated');
} else {
    console.error('[patch-homie-boolean] one or more anchors missing - NOT writing (homie-sdk layout changed?)');
    process.exit(1);
}
