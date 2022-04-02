#!/usr/bin/env bash

set -e
REPO_ROOT="$(cd "$(dirname "$0")/.."; pwd)"
BUILD_DIR="${REPO_ROOT}/build"
DIST_DIR="${REPO_ROOT}/dist"

# enable recursive match by **
cd "${REPO_ROOT}"

# check directories are already exist
if [ -e ${BUILD_DIR} ]; then
    echo "Build directory already exists: ${BUILD_DIR}"
    exit 1
elif [ -e ${DIST_DIR} ]; then
    echo "Dist directory already exists: ${DIST_DIR}"
    exit 1
fi

# build package
python setup.py sdist bdist_wheel
