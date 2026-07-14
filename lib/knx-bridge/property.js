const BasePropertyBridge = require('homie-sdk/lib/Bridge/BaseProperty');

class BaseProperty extends BasePropertyBridge {
    constructor(config, { type, transport, parser, debug }) {
        super(config, { type, transport, parser, debug });
        // handlers
    }
    // sync
    // async

    // In sidecar mode (KNX_DISABLE_POLL=1) the sidecar worker is the SOLE
    // publisher of relay state: its dedicated MQTT client reliably receives
    // every /set command and publishes the homie state topic immediately and
    // in order. The main homie process's own MQTT client, by contrast, is the
    // flaky one — it intermittently misses commands and delivers its publishes
    // seconds late and out of order, so a lagged publish of the PREVIOUS value
    // lands after the worker's fresh one and visibly reverts the UI toggle
    // ("работает через раз"). The base setValue publishes on every /set the
    // main process happens to receive; here we suppress that publish and keep
    // only the KNX bus write plus an in-memory model update (so a reconnect
    // republish still reflects the commanded value), leaving the worker as the
    // single, race-free publisher.
    async setValue(homieValue) {
        if (!this.settable) throw new Error('property is not settable.');

        const result = await this.transport.set(...this.parser.fromHomie(homieValue, this.transport.data));

        if (process.env.KNX_DISABLE_POLL === '1') {
            const homieVal = this.parser.toHomie(result);

            this.lastValue = homieVal;
            this.homieEntity.updateAttribute({ value: `${homieVal}` });

            return;
        }

        this.handleDataChanged(result, true);
    }

    // The base BasePropertyBridge.handlePublish re-asserts our own lastValue
    // whenever it observes a DIFFERENT value published to our value topic. In
    // sidecar mode the sidecar worker is the authoritative publisher, so when
    // the main process has missed a /set its lastValue is stale and this
    // "defend my value" logic fights the worker — re-publishing the previous
    // value right after the worker set the new one and visibly reverting the UI
    // toggle ("работает через раз"). Adopt the incoming value into lastValue
    // first so the re-assert sees no discrepancy and stays quiet; the main
    // process thus tracks the worker instead of overriding it.
    async handlePublish(data) {
        if (process.env.KNX_DISABLE_POLL === '1' && data && Object.keys(data)[0] === 'value') {
            this.lastValue = data.value;
        }
        return super.handlePublish(data);
    }
    // handlers~
    // ~handlers
}

module.exports = BaseProperty;
