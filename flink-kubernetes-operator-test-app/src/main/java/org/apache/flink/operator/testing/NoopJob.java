package org.apache.flink.operator.testing;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NoopJob {

    private NoopJob() {}

    public void execute() throws Exception {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(2000L);

        var stream =
                env.fromSequence(Long.MIN_VALUE, Long.MAX_VALUE)
                        .map(
                                l -> {
                                    Thread.sleep(1000L);
                                    return l;
                                });
        stream.addSink(
                new SinkFunction<>() {
                    @Override
                    public void invoke(Long value, Context context) {
                        // Do nothing
                    }
                });

        env.execute("Do nothing");
    }

    public static void main(String[] args) throws Exception {
        var job = new NoopJob();
        job.execute();
    }
}
