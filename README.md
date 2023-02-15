# Flink Kubernetes Operator Test Suite

I have tried to create some E2E test cases here for the Flink Kubernetes Operator that I found useful to test for every new release, but are hard to test manually.

The test suite will create the following services consisting of a single Pod and Service as well before running the tests:
- Kafka (to produce records for the Flink apps)
- Minio (durable S3 storage for Flink)

### Requirements
- Minikube server with the operator installed
- The test app compiled and image pushed to Minikube with the command:
```shell
docker build -t flink-kubernetes-operator-test-app:latest -f flink-kubernetes-operator-test-app/Dockerfile .
```