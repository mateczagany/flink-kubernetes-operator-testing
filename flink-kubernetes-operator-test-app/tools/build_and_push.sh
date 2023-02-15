#!/bin/sh

docker buildx build --push -t czmate10/flink-test-app-base -f flink-test-app-base.Dockerfile --platform linux/amd64,linux/arm64 .
