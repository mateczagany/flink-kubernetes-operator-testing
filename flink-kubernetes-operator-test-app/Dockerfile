FROM maven:3.9.8-eclipse-temurin-11 AS build
WORKDIR /app
COPY . .
RUN --mount=type=cache,target=/root/.m2 mvn -ntp clean install -pl flink-kubernetes-operator-test-app -am -DskipTests=true
RUN cp /app/flink-kubernetes-operator-test-app/target/flink-kubernetes-operator-test-app-*.jar app.jar

FROM flink:1.19
RUN mkdir -p ${FLINK_HOME}/plugins/flink-s3-fs-hadoop && cp ${FLINK_HOME}/opt/flink-s3-fs-hadoop-*.jar ${FLINK_HOME}/plugins/flink-s3-fs-hadoop
COPY --chown=flink:flink --from=build /app/app.jar /opt/flink/usrlib/app.jar