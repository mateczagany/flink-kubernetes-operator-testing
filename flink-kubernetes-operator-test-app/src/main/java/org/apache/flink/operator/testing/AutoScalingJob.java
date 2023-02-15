package org.apache.flink.operator.testing;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AutoScalingJob extends BaseJob {

    private AutoScalingJob(ParameterTool parameters) {
        super(parameters);
        env.enableCheckpointing(10000L, CheckpointingMode.EXACTLY_ONCE);
    }

    public void execute() throws Exception {
        source().uid("source")
                .setParallelism(1)
                .setMaxParallelism(1)
                .map(new DummyWaitRichMapFunction())
                .uid("dummy_wait")
                .sinkTo(sink())
                .uid("sink");
        env.execute(AutoScalingJob.class.getName());
    }

    public static void main(String[] args) throws Exception {
        var job = new AutoScalingJob(ParameterTool.fromArgs(args));
        job.execute();
    }

    static class DummyWaitRichMapFunction extends RichMapFunction<TestData, TestData> {
        @Override
        public TestData map(TestData testData) throws InterruptedException {
            log.info("Waiting for {} ms", testData.getSleepMillis());
            Thread.sleep(testData.getSleepMillis());
            return testData;
        }
    }
}
