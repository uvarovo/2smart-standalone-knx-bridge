# 2Smart Standalone KNX Bridge

Bridge between a KNX/IP gateway and the [2Smart Standalone](https://github.com/2SmartCloud/2smart-standalone-core) platform. Group addresses on the KNX bus are exposed as Homie-compatible sensors/options/telemetry via MQTT.

## Requirements

- Node.js 20 LTS (production image is built on `node:20-alpine`)
- A reachable KNX/IP gateway (IP address, port, physical address)
- An MQTT broker reachable from the bridge (the 2Smart stack provides EMQX)

## Configuration

All configuration is passed via environment variables. See [`2smart.configuration.json`](./2smart.configuration.json) for the canonical list rendered by the 2Smart admin UI.

| Variable | Required | Default | Description |
| --- | --- | --- | --- |
| `KNX_CONNECTION_IP_ADDR` | yes | — | IP of the KNX/IP gateway |
| `KNX_CONNECTION_IP_PORT` | no | `3671` | Gateway port |
| `KNX_CONNECTION_PHYS_ADDR` | yes | — | Physical address of the IP interface, e.g. `1.1.1` |
| `KNX_CONNECTION_LOCAL_IP` | yes | — | Local IP the bridge binds to |
| `KNX_CONNECTION_LOCAL_PORT_BINDING` | yes | `3672` | Local port, or `receivePort:listenPort` pair |
| `KNX_CONNECTION_FORCE_TUNNELING` | no | `true` | `true` or `false` |
| `DEVICE_NAME` | no | `KNX Bridge` | Homie device name |
| `MQTT_URI` / `MQTT_USER` / `MQTT_PASS` | yes | — | MQTT broker connection |
| `DEBUG` | no | — | Debug namespace filter (see [`homie-sdk/lib/utils/debugger`](https://github.com/2SmartCloud/2smart-standalone-homie-sdk)) |

Nodes / group-address mapping is supplied as a JSON blob in `etc/nodes.config.json` (mounted by the platform at install time).

## Local development

```bash
npm ci
npm run test:lint
node app.js
```

## Docker

```bash
docker build -t 2smart-standalone-knx-bridge .
docker run --rm --env-file ./.env 2smart-standalone-knx-bridge
```

The image is published to Docker Hub as `2smartdev/2smart-standalone-knx-bridge` and is installed through the **Market** → **Addons** → **KNX Bridge** flow of the admin UI.

## Continuous integration

- `.gitlab-ci.yml` — original GitLab pipeline (lint stage).
- `.github/workflows/lint.yml` — mirror for GitHub pull requests.

## License

See [`LICENSE.txt`](./LICENSE.txt) (English) and [`LICENSE_UA.txt`](./LICENSE_UA.txt) (Ukrainian).
