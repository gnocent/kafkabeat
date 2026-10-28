# Offline build from the vendored repository (make vendor). Pinned build
# image (Go and Alpine versions): bump it together with go.mod.
FROM golang:1.27.2-alpine3.23 AS build
ARG VERSION=dev
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 go build -mod=vendor -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /kafkabeat ./cmd/kafkabeat
RUN mkdir -p /out/var/lib/kafkabeat

FROM scratch
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /kafkabeat /kafkabeat
COPY kafkabeat.example.yml /etc/kafkabeat/kafkabeat.yml
# working directory writable by the container user: state.path ./state ends up there
COPY --from=build --chown=65532:65532 /out/var/lib/kafkabeat /var/lib/kafkabeat
USER 65532:65532
WORKDIR /var/lib/kafkabeat
VOLUME ["/var/lib/kafkabeat"]
ENTRYPOINT ["/kafkabeat"]
CMD ["-config", "/etc/kafkabeat/kafkabeat.yml"]
