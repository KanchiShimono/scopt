#!/usr/bin/env bash

set -e
REPO_ROOT="$(cd "$(dirname "$0")/.."; pwd)"

# enable recursive match by **
shopt -s globstar
cd "${REPO_ROOT}"

echo "Checking by black"
black --check --diff src tests

echo "Checking by isort"
isort ./**/*.py --check-only

echo "Checking by flake8"
flake8 src tests
