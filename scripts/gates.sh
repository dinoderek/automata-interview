#!/usr/bin/env bash
# Quality gates, run at the end of every round.
#
#   ./scripts/gates.sh            # static + test
#   ./scripts/gates.sh static     # gofmt, go vet, go build (no Docker needed)
#   ./scripts/gates.sh test       # go test -race against Postgres (docker compose)
#   ./scripts/gates.sh live       # rebuild executor, both acceptance workflows
#   ./scripts/gates.sh failure    # check-failure.sh (restarts incubator-1)
#   ./scripts/gates.sh all        # everything
#
# Stops at the first failing gate.
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
cd "$HERE/.."

step() { printf '\n==> %s\n' "$1"; }

static() {
  step "gofmt"
  unformatted=$(gofmt -l services)
  if [ -n "$unformatted" ]; then
    echo "not gofmt'd:"; echo "$unformatted"; exit 1
  fi
  step "go vet"
  go vet ./...
  step "go build"
  go build -o /dev/null ./services/executor ./services/worker
}

test_() {
  step "go test -race (docker compose run --rm tests)"
  docker compose up -d --wait postgres
  docker compose run --rm tests
}

live() {
  step "rebuild executor"
  docker compose up -d --build --wait executor
  step "acceptance: default workflow"
  ./scripts/acceptance.sh
  step "acceptance: Triple Assay"
  ./scripts/acceptance.sh "Triple Assay"
}

failure() {
  step "check-failure"
  ./scripts/check-failure.sh
}

case "${1:-default}" in
  default) static; test_ ;;
  static)  static ;;
  test)    test_ ;;
  live)    live ;;
  failure) failure ;;
  all)     static; test_; live; failure ;;
  *) echo "usage: $0 [static|test|live|failure|all]" >&2; exit 2 ;;
esac

printf '\nall requested gates passed\n'
