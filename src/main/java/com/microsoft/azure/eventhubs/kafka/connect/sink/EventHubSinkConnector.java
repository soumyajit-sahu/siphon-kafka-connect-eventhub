package com.microsoft.azure.eventhubs.kafka.connect.sink;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class EventHubSinkConnector extends SinkConnector {
    private static final Logger log = LoggerFactory.getLogger(EventHubSinkConnector.class);
    private Map<String, String> props;

    @Override
    public String version() {
        return getClass().getPackage().getImplementationVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        log.info("starting EventHubSinkConnector");
        this.props = props;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return EventHubSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        log.info("creating {} task configs", maxTasks);
        ArrayList<Map<String, String>> taskConfigs = new ArrayList<Map<String, String>>(maxTasks);
        for (int i = 0; i < maxTasks; i++) {
            taskConfigs.add(props);
        }
        return taskConfigs;
    }

    @Override
    public void stop() {
        log.info("stopping EventHubSinkConnector");
    }

    @Override
    public ConfigDef config() {
        return EventHubSinkConfig.CONFIG_DEF;
    }
}
