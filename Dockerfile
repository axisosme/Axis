# syntax=docker/dockerfile:1
FROM node:24.18.0-bookworm-slim AS build
WORKDIR /build
COPY package.json package-lock.json tsconfig.json tsconfig.build.json ./
COPY packages/contracts/ ./packages/contracts/
RUN npm ci --ignore-scripts --no-audit --no-fund
COPY src/ ./src/
RUN npm run build && npm prune --omit=dev --ignore-scripts --no-audit --no-fund

FROM node:24.18.0-bookworm-slim AS runtime
ENV NODE_ENV=production \
    HOME=/var/lib/axis/home \
    CODEX_HOME=/var/lib/axis/codex \
    PATH=/opt/axis/node_modules/.bin:$PATH
WORKDIR /opt/axis
COPY --from=build /build/package.json ./
COPY --from=build /build/dist/ ./dist/
COPY --from=build /build/node_modules/ ./node_modules/
COPY --from=build /build/packages/contracts/package.json ./packages/contracts/
COPY --from=build /build/packages/contracts/dist/src/ ./packages/contracts/dist/src/
COPY --from=build /build/packages/contracts/schemas/ ./packages/contracts/schemas/
COPY LICENSE ./
COPY deploy/docker/healthcheck.mjs ./deploy/docker/
RUN mkdir -p /etc/axis /var/lib/axis/data /var/lib/axis/workspaces /var/lib/axis/home /var/lib/axis/codex \
    && chown -R node:node /var/lib/axis \
    && chmod 0700 /var/lib/axis /var/lib/axis/data /var/lib/axis/home /var/lib/axis/codex \
    && ln -s /opt/axis/dist/bin/axis.js /usr/local/bin/axis \
    && chmod 0755 /opt/axis/dist/bin/axis.js
USER node
VOLUME ["/var/lib/axis"]
EXPOSE 8787
STOPSIGNAL SIGTERM
HEALTHCHECK --interval=30s --timeout=8s --start-period=60s --retries=3 \
    CMD ["node", "deploy/docker/healthcheck.mjs", "/etc/axis/axis.toml"]
ENTRYPOINT ["node", "dist/bin/axis.js"]
CMD ["serve", "--config", "/etc/axis/axis.toml"]
