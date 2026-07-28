FROM node:12.5-alpine

RUN apk update \
    && apk upgrade && apk add git

COPY etc etc
COPY lib lib
COPY patches patches
COPY package.json package.json
COPY package-lock.json package-lock.json
COPY app.js app.js
COPY app_set_sidecar.js app_set_sidecar.js
COPY sidecar_worker.js sidecar_worker.js

# npm ci respects package-lock.json so every build gets the exact same
# dependency tree. Without this, npm install with caret ranges (^1.0.0)
# would silently drift to newer minor versions of homie-sdk and break
# device $state semantics across rebuilds.
RUN npm ci --production

# Apply node_modules patches explicitly. `npm ci` runs as root with no WORKDIR
# here, so npm de-escalates lifecycle scripts to the "nobody" user and silently
# skips the `postinstall` hook ("cannot run in wd ... (wd=/)"). Running the
# patch scripts directly guarantees they are applied on every build. Both are
# idempotent and no-op if already patched.
RUN node patches/patch-knx-fsm.js && node patches/patch-homie-boolean.js

CMD npm start