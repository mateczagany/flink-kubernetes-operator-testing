package org.apache.flink.operator.testing;

public class Constants {
    public static final String KUBERNETES_NAMESPACE = "flink";
    protected static final String FLINK_IMAGE = "flink-kubernetes-operator-test-app:latest";
    public static final String TEST_JOB_JAR = "local:///opt/flink/usrlib/app.jar";
    public static final String TEST_JOB_ENTRYPOINT_HA =
            "org.apache.flink.operator.testing.HighAvailabilityJob";
    public static final String TEST_JOB_ENTRYPOINT_NOOP =
            "org.apache.flink.operator.testing.NoopJob";
    public static final String TEST_JOB_ENTRYPOINT_AUTO_SCALING =
            "org.apache.flink.operator.testing.AutoScalingJob";
}
