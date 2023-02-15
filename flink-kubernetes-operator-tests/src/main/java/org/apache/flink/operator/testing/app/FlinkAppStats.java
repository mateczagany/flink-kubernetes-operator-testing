package org.apache.flink.operator.testing.app;

import lombok.Builder;
import lombok.Getter;

import javax.annotation.Nullable;

@Builder
@Getter
public class FlinkAppStats {
    private String flinkVersion;
    private String jobId;
    private String jobStatus;
    private String jobManagerDeploymentStatus;
    private Long lastSavepointTimestamp;
    private int taskManagerCount;
    private String reconciliationState;
    @Nullable
    private String error;
}
