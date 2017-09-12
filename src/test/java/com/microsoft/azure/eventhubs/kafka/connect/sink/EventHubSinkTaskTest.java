package com.microsoft.azure.eventhubs.kafka.connect.sink;

import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventHubClient;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Spy;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class EventHubSinkTaskTest {

    @Spy
    public EventHubSinkTask spyEventHubSinkTask;

    @Before
    public void testSetup() throws Exception {
        // make EventHubSinkTask.sendAsync() method to not send to any real Event Hub
        CompletableFuture<Void> cf = new CompletableFuture<Void>();
        cf.complete(null);
        doReturn(cf).when(spyEventHubSinkTask).sendAsync(any(EventHubClient.class), any(EventData.class));
        doReturn(mock(EventHubClient.class)).when(spyEventHubSinkTask).getEventHubClientFromConnectionString(anyString());
    }

    @Test
    public void testEventHubClientSetup() {
        initConnectorTask("dummy-connection-string", (short) 2);
        assert spyEventHubSinkTask.getClientCount() == 2;
    }

    @Test
    public void testPutOfEventDataSinkRecords(){
        initConnectorTask("dummy-connection-string", (short) 5);
        spyEventHubSinkTask.put(getSinkRecords(20, true));
        verify(spyEventHubSinkTask, times(20)).sendAsync(isA(EventHubClient.class), isA(EventData.class));
    }

    @Test
    public void testPutOfBytesSinkRecords(){
        initConnectorTask("dummy-connection-string", (short) 5);
        spyEventHubSinkTask.put(getSinkRecords(20, false));
        verify(spyEventHubSinkTask, times(20)).sendAsync(isA(EventHubClient.class), isA(EventData.class));
    }

    private void initConnectorTask(String connString, short clientsPerTask) {
        spyEventHubSinkTask.stop(); // start clean slate
        Map<String, String> config = new HashMap<>();
        config.put(EventHubSinkConfig.CONNECTION_STRING, connString);
        config.put(EventHubSinkConfig.CLIENTS_PER_TASK, "" + clientsPerTask);

        spyEventHubSinkTask.start(config);
    }

    private List<SinkRecord> getSinkRecords(int count, boolean asEventData) {
        LinkedList<SinkRecord> recordList = new LinkedList<>();
        for(int i = 0; i < count; i++) {
            recordList.add(new SinkRecord("topic1", -1, null, null, null,
                    asEventData? new EventData("testdata1".getBytes()) : "testdata1".getBytes(), -1));
        }
        return recordList;
    }
}
