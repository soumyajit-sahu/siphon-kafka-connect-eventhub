package com.microsoft.kafkaconnectors.sinkconnector;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class EventHubSinkConnector extends SinkConnector {

    public static final String CONNECTION_STRING = "eventhub.connection.string";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(CONNECTION_STRING, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Connection string of the EventHub")
            ;

    private Map<String, Object> configProperties;

    @Override
    public String version() {
        return "1.0";
    }

    @Override
    public void start(Map<String, String> props) {
        configProperties = CONFIG_DEF.parse(props);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return EventHubSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        ArrayList<Map<String, String>> configs = new ArrayList<Map<String, String>>();
        for (int i = 0; i < maxTasks; i++) {
            Map<String, String> props = new HashMap<String, String>();
            props.put(CONNECTION_STRING, configProperties.get(CONNECTION_STRING).toString());
            configs.add(props);
        }
        return configs;
    }

    @Override
    public void stop() {
        // do nothing
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }
}
