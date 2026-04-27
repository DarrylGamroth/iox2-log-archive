#!/usr/bin/env bash
set -euo pipefail

if [[ $# -eq 0 ]]; then
    mkdir -p target/llvm-cov
    set -- --lcov --output-path target/llvm-cov/lcov.info
fi

cargo llvm-cov --workspace --all-targets --no-fail-fast "$@"
