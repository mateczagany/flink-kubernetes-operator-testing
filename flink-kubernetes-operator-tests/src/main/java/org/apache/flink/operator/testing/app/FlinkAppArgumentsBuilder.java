package org.apache.flink.operator.testing.app;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FlinkAppArgumentsBuilder {
    private final List<String> arguments = new ArrayList<>();

    public FlinkAppArgumentsBuilder addProperties(Map<String, String> properties) {
        for (var entry : properties.entrySet()) {
            arguments.add(String.format("--%s", entry.getKey()));
            arguments.add(entry.getValue());
        }
        return this;
    }

    public FlinkAppArgumentsBuilder addProperty(String key, String value) {
        arguments.add(String.format("--%s", key));
        arguments.add(value);
        return this;
    }

    public String[] build() {
        return arguments.toArray(String[]::new);
    }
}
