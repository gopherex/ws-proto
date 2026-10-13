SHELL := bash
.ONESHELL:
.SHELLFLAGS := -eu -o pipefail -c

ROOT_MODULE := github.com/gopherex/ws-proto
# Max major allowed. v2+ needs semantic import versioning (/vN in the module
# path), which we don't support yet — keep releases on v0/v1.
MAX_MAJOR := 1

.PHONY: help gen build-gen build-gen-rs gen-example gen-rs-golden fmt test test-ts test-rs test-interop release

help:
	@echo "make gen          - regenerate transport/*.pb.go + crates/ws-proto-frame via easyp"
	@echo "make build-gen    - build the protoc-gen-go-ws plugin into bin/"
	@echo "make build-gen-rs - build the protoc-gen-ws-rs plugin (cargo, debug)"
	@echo "make gen-example  - regenerate the golden example/"
	@echo "make gen-rs-golden- regenerate crates/protoc-gen-ws-rs/test/gen (prost + ws-rs + prost-crate)"
	@echo "make test         - gofmt check + go vet + go test ./..."
	@echo "make test-ts      - build + test the npm workspaces"
	@echo "make test-rs      - cargo fmt/clippy/test + build the generated golden crate"
	@echo "make test-interop - cross-language matrix: Rust<->Go, Go->Rust, TS->Rust over real sockets"
	@echo "make release      - interactive tag-driven release (Go module + npm packages share one version)"

# --- codegen / build ---------------------------------------------------------

# Needs protoc-gen-go (Go) and protoc-gen-prost + protoc-gen-prost-crate +
# protoc-gen-tonic (`cargo install protoc-gen-prost protoc-gen-prost-crate
# protoc-gen-tonic`) on PATH.
gen:
	easyp generate

build-gen:
	go build -o bin/protoc-gen-go-ws ./cmd/protoc-gen-go-ws

build-gen-rs:
	cargo build -p protoc-gen-ws-rs

gen-example: build-gen build-gen-rs
	cd example && PATH="$(CURDIR)/bin:$(CURDIR)/target/debug:$$PATH" easyp generate

gen-rs-golden: build-gen-rs
	cd crates/protoc-gen-ws-rs && rm -rf test/gen && easyp generate

fmt:
	gofmt -w .

# --- test --------------------------------------------------------------------

test:
	gofmt -l .
	go vet ./...
	go test -race ./...

test-ts:
	npm ci
	npm run -ws build
	npm run -ws test

# Hand-written crates only: ws-proto-frame and example/rust/echo-proto are generated.
RS_CRATES := -p ws-proto -p protoc-gen-ws-rs -p ws-proto-example-server

test-rs: gen-rs-golden
	cargo fmt $(RS_CRATES) -- --check
	cargo clippy --workspace --all-targets --all-features -- -D warnings
	cargo test --workspace --all-features
	cargo build -p ws-proto --no-default-features
	cd crates/protoc-gen-ws-rs/test/gen && cargo build

# Cross-language matrix over real TCP WebSockets. `cargo test --workspace`
# already covers Rust client -> Go server (example/rust/server/tests); this adds
# the Go and TS clients against the Rust server binary.
test-interop:
	cargo build -p ws-proto-example-server
	cargo test -p ws-proto-example-server --test interop
	WS_PROTO_RUST_SERVER="$(CURDIR)/target/debug/ws-proto-example-server" go test ./example -run TestRustServer -v -count=1
	WS_PROTO_IMPL=tonic WS_PROTO_RUST_SERVER="$(CURDIR)/target/debug/ws-proto-example-server" go test ./example -run TestRustServer -v -count=1
	WS_PROTO_SERVER_BIN="$(CURDIR)/target/debug/ws-proto-example-server" npm run -w packages/protoc-gen-ws-es test -- integration

# --- release -----------------------------------------------------------------
# One version drives everything: the Go module is tagged vX.Y.Z, both npm
# packages (@gopherex/ws-proto-transport, @gopherex/protoc-gen-ws-es) and the
# cargo workspace ([workspace.package] version → all crates) are bumped to the
# same X.Y.Z. Pushing the tag triggers .github/workflows/release.yml, which
# builds the Go plugin binaries and publishes the npm packages to GitHub Packages.
release:
	@set -euo pipefail
	cd "$$(git rev-parse --show-toplevel)"

	if [ -n "$$(git status --porcelain)" ]; then
	  echo "✗ Working tree is not clean — commit or stash first:"
	  git status --short
	  exit 1
	fi

	cur="$$(git tag -l 'v[0-9]*.[0-9]*.[0-9]*' | sed 's/^v//' | sort -t. -k1,1n -k2,2n -k3,3n | tail -1)"
	cur="$${cur:-0.0.0}"
	head="$$(git rev-parse --short HEAD)"
	echo "Latest release: v$$cur    HEAD: $$head"
	echo
	echo "  1) bump version"
	echo "  2) recreate last tag (v$$cur) on HEAD   [force]"
	echo "  3) cancel"
	read -r -p "> " action

	set_npm_version() { # $1 = version (without v); bumps both workspaces + lockfile
	  npm version "$$1" --no-git-tag-version --workspaces >/dev/null
	}
	set_cargo_version() { # $1 = version; root [workspace.package] + intra-workspace path deps + template + lockfile
	  sed -i -E "0,/^version = \"[0-9.]+\"/s//version = \"$$1\"/" Cargo.toml
	  sed -i -E "s/(ws-proto-frame = \{ version = )\"[0-9.]+\"/\1\"$$1\"/" crates/*/Cargo.toml
	  cargo update --workspace --offline >/dev/null
	}

	case "$$action" in
	1)
	  IFS=. read -r MA MI PA <<< "$$cur"
	  echo
	  echo "  1) major  -> v$$((MA+1)).0.0"
	  echo "  2) minor  -> v$$MA.$$((MI+1)).0"
	  echo "  3) patch  -> v$$MA.$$MI.$$((PA+1))"
	  read -r -p "> " comp
	  case "$$comp" in
	    1) MA=$$((MA+1)); MI=0; PA=0 ;;
	    2) MI=$$((MI+1)); PA=0 ;;
	    3) PA=$$((PA+1)) ;;
	    *) echo "Aborted."; exit 0 ;;
	  esac
	  if [ "$$MA" -gt "$(MAX_MAJOR)" ]; then
	    echo "✗ v$$MA requires semantic import versioning (/v$$MA in the module path)."
	    echo "  Not supported yet — stay on v0/v1."
	    exit 1
	  fi
	  new="$$MA.$$MI.$$PA"
	  echo
	  echo "Release v$$new — will:"
	  echo "  - set npm version $$new in both packages (+ lockfile)"
	  echo "  - set cargo workspace version $$new (+ Cargo.lock)"
	  echo "  - commit 'release v$$new'"
	  echo "  - create tag v$$new and push HEAD + tag (triggers CI release)"
	  read -r -p "Type 'yes' to proceed: " ok
	  [ "$$ok" = "yes" ] || { echo "Aborted."; exit 0; }

	  set_npm_version "$$new"
	  set_cargo_version "$$new"
	  git add -A
	  git diff --cached --quiet || git commit -m "release v$$new"
	  git tag -a "v$$new" -m "v$$new"
	  git push origin HEAD
	  git push origin "v$$new"
	  echo "✓ Released v$$new."
	  ;;
	2)
	  if [ "$$cur" = "0.0.0" ] && ! git tag -l 'v0.0.0' | grep -q .; then
	    echo "✗ No release tag to recreate."; exit 1
	  fi
	  echo
	  echo "Will DELETE and recreate tag v$$cur on $$head, then force-push."
	  read -r -p "Type 'yes' to proceed: " ok
	  [ "$$ok" = "yes" ] || { echo "Aborted."; exit 0; }
	  git tag -d "v$$cur" 2>/dev/null || true
	  git push origin ":refs/tags/v$$cur" 2>/dev/null || true
	  git tag -a "v$$cur" -m "v$$cur"
	  git push origin --force "v$$cur"
	  echo "✓ Recreated v$$cur on $$head."
	  ;;
	*)
	  echo "Cancelled."
	  ;;
	esac
