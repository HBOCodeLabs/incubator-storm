package storm.kafka;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.RETURNS_SMART_NULLS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import backtype.storm.spout.SpoutOutputCollector;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.MessageAndOffset;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PartitionManagerTest {

    private static final String topologyInstanceId = "testTopInstId0001";
    private static final String topicName = "testTopicName";
    private static final String zkRoot = "/consumerRoot";
    private static final String consumerId = "testConsumer";
    private static final long lastOffset = 1000L;
    private List<Object> tuple = Arrays.asList((Object) "test msg");
    private Partition partition;
    private KafkaUtils kafkaUtils;

    public PartitionManager setupPartitionManagerWithMocks(boolean backoff) {
        DynamicPartitionConnections connections = mock(DynamicPartitionConnections.class, RETURNS_SMART_NULLS);
        SimpleConsumer consumer = mock(SimpleConsumer.class, RETURNS_SMART_NULLS);
        ZkState zkstate = mock(ZkState.class, RETURNS_SMART_NULLS);
        Map<String,String> stormConf = new HashMap<String, String>();
        ZkHosts zkhosts = mock(ZkHosts.class, RETURNS_SMART_NULLS);
        SpoutConfig spoutConfig = new SpoutConfig(zkhosts, topicName, zkRoot, consumerId);
        if (backoff) {
            spoutConfig.retryInitialDelayMs = 10L;
            spoutConfig.retryDelayMultiplier = 2.0d;
            spoutConfig.retryDelayMaxMs = 40L;
        }
        Broker broker = mock(Broker.class, RETURNS_SMART_NULLS);
        this.partition = new Partition(broker, 0);
        this.kafkaUtils = getMockKafkaUtils(consumer, spoutConfig);

        when(connections.register(broker, 0)).thenReturn(consumer);
        when(zkstate.readJSON(anyString())).thenReturn(new HashMap<Object,Object>() {{
                this.put("offset", lastOffset);
            }});
        when(zkstate.readJSON(anyString())).thenReturn(null);

        return new PartitionManager(connections, topologyInstanceId, zkstate,
                                    stormConf, spoutConfig, this.partition, kafkaUtils);
    }

    private KafkaUtils getMockKafkaUtils(SimpleConsumer consumer, SpoutConfig spoutConfig) {
        KafkaUtils kafkaUtils = mock(KafkaUtils.class, RETURNS_SMART_NULLS);
        when(kafkaUtils.getOffset(consumer, topicName, 0, spoutConfig)).thenReturn(lastOffset);
        when(kafkaUtils.fetchMessages(any(SpoutConfig.class), any(SimpleConsumer.class),
                                      any(Partition.class), anyLong())).thenAnswer(new Answer<ByteBufferMessageSet>() {
            @Override
            public ByteBufferMessageSet answer(InvocationOnMock invocation) throws Throwable {
                Message msg = new Message(ByteBuffer.wrap("{ \"str\": \"test msg\" }".getBytes()));
                ByteBufferMessageSet messageSet = mock(ByteBufferMessageSet.class, RETURNS_SMART_NULLS);
                MessageAndOffset msgAndOffset = new MessageAndOffset(msg, (Long) invocation.getArguments()[3]);
                when(messageSet.iterator()).thenReturn(Arrays.asList(msgAndOffset).iterator());
                return messageSet;
            }
        });

        when(kafkaUtils.generateTuples(any(SpoutConfig.class), any(Message.class))).thenReturn((Arrays.asList(this.tuple)));
        return kafkaUtils;
    }

    @Test
    public void testNext() throws Exception {
        // mocks
        PartitionManager partitionManager = setupPartitionManagerWithMocks(false);
        SpoutOutputCollector outputCollector = mock(SpoutOutputCollector.class, RETURNS_SMART_NULLS);

        // run test
        partitionManager.next(outputCollector);

        // validate
        // should request message at offset lastOffset
        verify(kafkaUtils).fetchMessages(any(SpoutConfig.class), any(SimpleConsumer.class),
                                         any(Partition.class), eq(lastOffset));

        ArgumentCaptor<List<Object>> tupleCaptor = new ArgumentCaptor<List<Object>>();
        verify(outputCollector, times(1)).emit(tupleCaptor.capture(), any());
        assertSame("expect emit tuple to the OutputCollector", this.tuple, tupleCaptor.getValue());
    }

    @Test
    public void testAck() throws Exception {
        // mocks
        PartitionManager partitionManager = setupPartitionManagerWithMocks(false);

        // run test
        partitionManager.ack(lastOffset);

        // validate
        assertEquals("expect message removed from _pending", lastOffset, partitionManager.lastCompletedOffset());
    }

    @Test
    public void testNextAfterFail() throws Exception {
        // mocks
        Long failedMsgOffset = 101L;
        PartitionManager partitionManager = setupPartitionManagerWithMocks(false);
        SpoutOutputCollector outputCollector = mock(SpoutOutputCollector.class, RETURNS_SMART_NULLS);

        // run test
        partitionManager.fail(failedMsgOffset);
        partitionManager.next(outputCollector);

        // validate
        // should request message at failed message offset
        verify(kafkaUtils).fetchMessages(any(SpoutConfig.class), any(SimpleConsumer.class),
                                         any(Partition.class), eq(failedMsgOffset));

        ArgumentCaptor<List<Object>> tupleCaptor = new ArgumentCaptor<List<Object>>();
        verify(outputCollector, times(1)).emit(tupleCaptor.capture(), any());
        assertSame("expect emit tuple to the OutputCollector", this.tuple, tupleCaptor.getValue());
    }

    @Test
    public void testNextAfterFailButBeforeReady() throws Exception {
        // mocks
        Long failedMsgOffset = 101L;
        PartitionManager partitionManager = setupPartitionManagerWithMocks(true);
        SpoutOutputCollector outputCollector = mock(SpoutOutputCollector.class, RETURNS_SMART_NULLS);

        // run test
        partitionManager.fail(failedMsgOffset);
        partitionManager.next(outputCollector);

        // validate
        // should request message at offset lastOffset - failed msg not ready yet
        verify(kafkaUtils).fetchMessages(any(SpoutConfig.class), any(SimpleConsumer.class),
                                         any(Partition.class), eq(lastOffset));

        ArgumentCaptor<List<Object>> tupleCaptor = new ArgumentCaptor<List<Object>>();
        verify(outputCollector, times(1)).emit(tupleCaptor.capture(), any());
        assertSame("expect emit tuple to the OutputCollector", this.tuple, tupleCaptor.getValue());
    }

    @Test
    public void testNextAfterFailAndAfterReady() throws Exception {
        // mocks
        Long failedMsgOffset = 101L;
        PartitionManager partitionManager = setupPartitionManagerWithMocks(true);
        SpoutOutputCollector outputCollector = mock(SpoutOutputCollector.class, RETURNS_SMART_NULLS);

        // run test
        partitionManager.fail(failedMsgOffset);
        Thread.sleep(15L);
        partitionManager.next(outputCollector);

        // validate
        // should request message at failed message offset
        verify(kafkaUtils).fetchMessages(any(SpoutConfig.class), any(SimpleConsumer.class),
                                         any(Partition.class), eq(failedMsgOffset));

        ArgumentCaptor<List<Object>> tupleCaptor = new ArgumentCaptor<List<Object>>();
        verify(outputCollector, times(1)).emit(tupleCaptor.capture(), any());
        assertSame("expect emit tuple to the OutputCollector", this.tuple, tupleCaptor.getValue());
    }

    @Test
    public void testBackoff() throws Exception {
        // mocks
        Long failedMsgOffset = 101L;
        PartitionManager partitionManager = setupPartitionManagerWithMocks(true);
        SpoutOutputCollector outputCollector = mock(SpoutOutputCollector.class, RETURNS_SMART_NULLS);

        // run test
        partitionManager.fail(failedMsgOffset);
        Thread.sleep(15L);
        partitionManager.next(outputCollector);

        // validate
        // should request message at failed message offset
        verify(kafkaUtils).fetchMessages(any(SpoutConfig.class), any(SimpleConsumer.class),
                                         any(Partition.class), eq(failedMsgOffset));


        // run test
        partitionManager.fail(failedMsgOffset);
        Thread.sleep(15L);
        partitionManager.next(outputCollector);

        // validate
        // should request message at offset lastOffset - failed msg not ready yet
        verify(kafkaUtils).fetchMessages(any(SpoutConfig.class), any(SimpleConsumer.class),
                                         any(Partition.class), eq(lastOffset));

        // run test
        Thread.sleep(5L);
        partitionManager.next(outputCollector);

        // validate
        // should request message at failed message offset
        verify(kafkaUtils, times(2)).fetchMessages(any(SpoutConfig.class), any(SimpleConsumer.class),
                                                   any(Partition.class), eq(failedMsgOffset));

        // run test
        partitionManager.fail(failedMsgOffset);
        Thread.sleep(30L);
        partitionManager.next(outputCollector);

        // validate
        // should request message at offset lastOffset - failed msg not ready yet
        verify(kafkaUtils).fetchMessages(any(SpoutConfig.class), any(SimpleConsumer.class),
                                         any(Partition.class), eq(lastOffset + 1));

        // run test
        Thread.sleep(10L);
        partitionManager.next(outputCollector);

        // validate
        // should request message at failed message offset
        verify(kafkaUtils, times(3)).fetchMessages(any(SpoutConfig.class), any(SimpleConsumer.class),
                                                   any(Partition.class), eq(failedMsgOffset));
    }
}