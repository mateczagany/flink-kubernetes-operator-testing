package org.apache.flink.operator.testing;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HighAvailabilityJob extends BaseJob {

    private HighAvailabilityJob(ParameterTool parameters) {
        super(parameters);
        env.enableCheckpointing(2000L);

        var stateBackend = parameters.get("flink.state.backend", "memory");
        if ("hashmap".equals(stateBackend)) {
            env.setStateBackend(new HashMapStateBackend());
        } else if ("rocks".equals(stateBackend)) {
            env.setStateBackend(new EmbeddedRocksDBStateBackend(true));
        }
    }

    public void execute() throws Exception {
        // TODO: windowing
        source().sinkTo(sink());
        env.execute(HighAvailabilityJob.class.getName());
    }

    public static void main(String[] args) throws Exception {
        var job = new HighAvailabilityJob(ParameterTool.fromArgs(args));
        job.execute();
    }
}
