package com.linkedin.camus.etl.kafka;

import com.linkedin.camus.etl.kafka.common.KafkaReader;
import org.apache.kafka.common.TopicPartition;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map.Entry;

public class KafkaReaderTest {

    @Test
    public void testParseKafkaException() {
        String error = "Record batch for partition glup_bid_request-1318 at offset 24173647363 is invalid, cause: Record is corrupt (stored crc = 1686744375, computed crc = 3335677880)";
        Entry<TopicPartition, Long> offset = KafkaReader.parseErrorString(KafkaReader.BAD_MASSAGE_PATTERN, error);
        Assert.assertEquals(offset.getKey().topic(), "glup_bid_request");
        Assert.assertEquals(offset.getKey().partition(), 1318);
        Assert.assertEquals(offset.getValue().longValue(), 24173647363L);
    }
}
