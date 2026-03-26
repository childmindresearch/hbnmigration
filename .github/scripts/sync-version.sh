#!/bin/bash
set -e

VERSION=$(cat VERSION | tr -d '[:space:]')

if [ -z "$VERSION" ]; then
    echo "ERROR: VERSION file is empty"
    exit 1
fi

echo "Syncing version to: $VERSION"

# Copy VERSION to python_jobs
cp VERSION python_jobs/VERSION
echo "✓ Copied VERSION to python_jobs/"

# Update root package.json
if [ -f package.json ]; then
    jq --arg version "$VERSION" '.version = $version' package.json > package.json.tmp && \
    mv package.json.tmp package.json
    echo "✓ Updated package.json"
fi

# Update javascript_jobs
if [ -f javascript_jobs/package.json ]; then
    jq --arg version "$VERSION" '.version = $version' javascript_jobs/package.json > javascript_jobs/package.json.tmp && \
    mv javascript_jobs/package.json.tmp javascript_jobs/package.json
    echo "✓ Updated javascript_jobs/package.json"
fi

echo "✓ All versions synced to $VERSION"
