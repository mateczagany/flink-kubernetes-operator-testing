package org.apache.flink.operator.testing;

public class Constants {
    public static final String KUBERNETES_CONTAINER_FLINK_NAME = "flink-main-container";
    public static final String KUBERNETES_NAMESPACE = "flink";
    public static final String TEST_JOB_JAR = "local:///opt/app/flink-test-app.jar";
    public static final String TEST_JOB_ENTRYPOINT_HA = "org.apache.flink.operator.testing.HighAvailabilityJob";
    public static final String TEST_JOB_ENTRYPOINT_NOOP = "org.apache.flink.operator.testing.NoopJob";
    public static final String TEST_JOB_ENTRYPOINT_AUTO_SCALING = "org.apache.flink.operator.testing.AutoScalingJob";
}
