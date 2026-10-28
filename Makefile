# Targets to run on Linux (WSL included): make check before every commit.
GO      ?= go
BIN     := bin/kafkabeat
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMPOSE := docker compose -f bench/compose.yaml
# Jolokia JVM agents for the bench brokers (broker metricset), downloaded once
# from Maven Central and verified by their SHA-256 sum (computed on jars whose
# SHA-1 sum published by Maven Central had been checked): 1.x on Kafka 3.9,
# 2.x on Kafka 4.4 (the two generations answer differently for a missing MBean).
JOLOKIA_VERSION := 2.6.3
JOLOKIA_SHA256  := b51f95db08cc44c112a3724e9c782ea7f9cf60de5fb70cced119832bda17dadf
JOLOKIA_JAR     := bench/jolokia/jolokia-agent.jar
JOLOKIA1_VERSION := 1.7.2
JOLOKIA1_SHA256  := 65d5645b07f7bdc22c7a1dbf060b5345b564674cc263d14f75ade2dabe80f1f2
JOLOKIA1_JAR     := bench/jolokia/jolokia-agent-1.jar

.PHONY: check build vet test bench-up bench-down recette vendor dist image load kibana demo

check: vet test build

build:
	$(GO) build -trimpath -ldflags "-s -w -X main.version=$(VERSION)" -o $(BIN) ./cmd/kafkabeat

vet:
	$(GO) vet ./...

test:
	$(GO) test -count=1 ./...

bench-up: $(JOLOKIA_JAR) $(JOLOKIA1_JAR)
	$(COMPOSE) up -d --wait

$(JOLOKIA_JAR):
	mkdir -p $(dir $@)
	curl -sfL -o $@.tmp https://repo1.maven.org/maven2/org/jolokia/jolokia-agent-jvm/$(JOLOKIA_VERSION)/jolokia-agent-jvm-$(JOLOKIA_VERSION)-javaagent.jar
	echo "$(JOLOKIA_SHA256)  $@.tmp" | sha256sum -c --quiet
	mv $@.tmp $@

$(JOLOKIA1_JAR):
	mkdir -p $(dir $@)
	curl -sfL -o $@.tmp https://repo1.maven.org/maven2/org/jolokia/jolokia-jvm/$(JOLOKIA1_VERSION)/jolokia-jvm-$(JOLOKIA1_VERSION).jar
	echo "$(JOLOKIA1_SHA256)  $@.tmp" | sha256sum -c --quiet
	mv $@.tmp $@

bench-down:
	$(COMPOSE) down -v

recette: bench/render/node_modules
	$(GO) test -tags bench -count=1 -timeout 150m -v ./recette/...

# Dashboard rendering tool (criteria 8 and 9): playwright-core installs no
# browser, the Chromium of the Playwright cache is downloaded separately
# (idempotent; on a fresh Ubuntu, add --with-deps for the system libraries).
bench/render/node_modules: bench/render/package-lock.json
	cd bench/render && npm ci --no-audit --no-fund && npx playwright-core install chromium
	touch $@

# Records demokafkabeat.gif, the README demo, on the running bench (about 7 min).
demo: bench/render/node_modules
	bench/demo.sh

# Recreates the Kibana examples (data views, dashboards, rule) in the bench Kibana and regenerates the export.
kibana:
	python3 kibana/examples.py

vendor:
	$(GO) mod vendor

# Release archives in dist/: static binary, documentation, example configuration,
# deployment files (systemd, Nomad) and Kibana examples, for each target; tar.gz,
# zip for Windows, and their SHA256SUMS.
DIST_TARGETS := linux/amd64 darwin/arm64 windows/amd64
DIST_FILES   := kafkabeat.example.yml LICENSE README.md README_fr.md INSTALL.md INSTALL_fr.md \
                USAGE.md USAGE_fr.md DESIGN.md DESIGN_fr.md CHANGELOG.md CHANGELOG_fr.md \
                SECURITY.md SECURITY_fr.md deploy/kafkabeat.service deploy/kafkabeat.nomad.hcl \
                kibana/kafkabeat-examples.ndjson

dist: vendor
	rm -rf dist && mkdir -p dist
	set -e; for t in $(DIST_TARGETS); do \
	  os=$${t%/*}; arch=$${t#*/}; ext=; if [ $$os = windows ]; then ext=.exe; fi; \
	  stage=dist/stage/$$os-$$arch; d=$$stage/kafkabeat-$(VERSION); mkdir -p $$d; \
	  CGO_ENABLED=0 GOOS=$$os GOARCH=$$arch $(GO) build -mod=vendor -trimpath \
	    -ldflags "-s -w -X main.version=$(VERSION)" -o $$d/kafkabeat$$ext ./cmd/kafkabeat; \
	  cp $(DIST_FILES) $$d/; \
	  if [ $$os = windows ]; then \
	    (cd $$stage && python3 -m zipfile -c ../../kafkabeat-$(VERSION)-$$os-$$arch.zip kafkabeat-$(VERSION)); \
	  else \
	    tar -C $$stage -czf dist/kafkabeat-$(VERSION)-$$os-$$arch.tar.gz kafkabeat-$(VERSION); \
	  fi; \
	done
	rm -rf dist/stage
	cd dist && sha256sum kafkabeat-* > SHA256SUMS && cat SHA256SUMS

image: vendor
	docker build --build-arg VERSION=$(VERSION) -t kafkabeat:$(VERSION) .

load:
	KAFKABEAT_LOAD=1 $(GO) test -count=1 -run TestLoad30k -v ./internal/collector/
