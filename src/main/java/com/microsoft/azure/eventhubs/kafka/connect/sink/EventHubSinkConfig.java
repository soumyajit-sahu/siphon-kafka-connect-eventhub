package com.microsoft.azure.eventhubs.kafka.connect.sink;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Importance;

import java.util.Map;

public class EventHubSinkConfig extends AbstractConfig {
    public static final String CONNECTION_STRING = "eventhub.connection.string";
    public static final String CLIENTS_PER_TASK = "eventhub.clients.per.task";

    private static final short defaultClientsPerTask = 1;

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(CONNECTION_STRING, Type.STRING, Importance.HIGH,
                    "EventHub Connection String")
            .define(CLIENTS_PER_TASK, Type.SHORT, defaultClientsPerTask, Importance.HIGH,
                    "Number of Event Hub clients to use per task");

    public EventHubSinkConfig(Map<String, String> configValues) {
        super(CONFIG_DEF, configValues);
    }
}
