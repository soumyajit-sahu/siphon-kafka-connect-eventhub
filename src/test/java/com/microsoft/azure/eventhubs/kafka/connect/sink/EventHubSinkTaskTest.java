package com.microsoft.azure.eventhubs.kafka.connect.sink;

import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventHubClient;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class EventHubSinkTaskTest {

    @Spy
    public EventHubSinkTask spyEventHubSinkTask;

    @Before
    public void testSetup() {
        // make EventHubSinkTask.sendAsync() method to not send to any real Event Hub
        CompletableFuture<Void> cf = new CompletableFuture<Void>();
        cf.complete(null);
        doReturn(cf).when(spyEventHubSinkTask).sendAsync(any(EventHubClient.class), any(EventData.class));

        initEventHubClients(5);
    }

    @Test
    public void testPutOfSinkRecords(){
        spyEventHubSinkTask.put(getSinkRecords(20));
        verify(spyEventHubSinkTask, times(20)).sendAsync(isA(EventHubClient.class), isA(EventData.class));
    }

    private void initEventHubClients(int count) {
        spyEventHubSinkTask.ehClients = new LinkedBlockingQueue<EventHubClient>(count);
        for(int i = 0; i < count; i++) {
            spyEventHubSinkTask.ehClients.offer(mock(EventHubClient.class));
        }
    }

    private List<SinkRecord> getSinkRecords(int count) {
        LinkedList<SinkRecord> recordList = new LinkedList<>();
        for(int i = 0; i < count; i++) {
            recordList.add(new SinkRecord("topic1", -1, null, null,
                    null, new EventData("testdata1".getBytes()), -1));
        }
        return recordList;
    }
}
