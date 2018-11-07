package com.linkedin.camus.etl.kafka.common;

import com.linkedin.camus.etl.kafka.CamusJob;
import com.linkedin.camus.etl.kafka.mapred.EtlInputFormat;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicReference;


/**
 * Poorly named class that handles kafka pull events within each
 * KafkaRecordReader.
 *
 * @author Richard Park
 */
public class KafkaReader {
  // index of context
  private static Logger log = Logger.getLogger(KafkaReader.class);
  private EtlRequest kafkaRequest = null;
  private KafkaConsumer<byte[], byte[]> consumer = null;

  private long beginOffset;
  private long currentOffset;
  private long lastOffset;
  private long currentCount;

  private TaskAttemptContext context;

  private Iterator<ConsumerRecord<byte[], byte[]>> messageIter = null;

  private long totalFetchTime = 0;
  private long lastFetchTime = 0;
  private final long pollTimeoutMs;

  /**
   * Construct using the json representation of the kafka request
   */
  public KafkaReader(EtlInputFormat inputFormat, TaskAttemptContext context, EtlRequest request) {
    this.context = context;

    // Create the kafka request from the json
    kafkaRequest = request;

    beginOffset = request.getOffset();
    currentOffset = request.getOffset();
    lastOffset = request.getLastOffset();
    currentCount = 0;
    totalFetchTime = 0;
    pollTimeoutMs = CamusJob.getKafkaConsumerPollTimeoutMs(context);

    // read data from queue
    consumer = inputFormat.createKafkaConsumer(context);
    TopicPartition topicPartition = new TopicPartition(kafkaRequest.getTopic(), kafkaRequest.getPartition());
    consumer.assign(Collections.singletonList(topicPartition));
    consumer.seek(topicPartition, kafkaRequest.getOffset());
    log.info("Beginning reading at offset " + beginOffset + " latest offset="
        + lastOffset);
    fetch();
  }

  public boolean hasNext() throws IOException {
    if (messageIter != null && messageIter.hasNext())
      return true;
    else
      return fetch();

  }

  /**
   * Fetches the next Kafka message and stuffs the results into the key and
   * value
   *
   * @param key
   * @param payload
   * @param pKey
   * @return true if there exists more events
   */
  public boolean getNext(EtlKey key, BytesWritable payload, BytesWritable pKey,
                         AtomicReference<ConsumerRecord<byte[], byte[]>> recordRef) throws IOException, MalformedMessageException {
    if (!hasNext()) return false;

    ConsumerRecord<byte[], byte[]> record = messageIter.next();
    if (record.value() == null) {
      throw new MalformedMessageException("Null record value for offset " + record.offset());
    }
    recordRef.set(record);

    try {
      payload.set(record.value(), 0, record.value().length);

      if (record.key() != null) {
        pKey.set(record.key(), 0, record.key().length);
      }

      return true;
    } catch (Exception e) {
      throw new MalformedMessageException("Malformed message: " + Arrays.toString(record.value()), e);
    } finally {
      key.clear();
      key.set(kafkaRequest.getTopic(), kafkaRequest.getLeaderId(), kafkaRequest.getPartition(), currentOffset, record.offset() + 1, record.checksum());

      key.setMessageSize(record.serializedKeySize() + record.serializedValueSize());

      currentOffset = record.offset() + 1; // increase offset
      currentCount++; // increase count
    }
  }

  /**
   * Creates a fetch request.
   *
   * @return false if there's no more fetches
   * @throws IOException
   */

  public boolean fetch() {
    if (currentOffset >= lastOffset) {
      return false;
    }

    long tempTime = System.currentTimeMillis();
    try {
      ConsumerRecords<byte[], byte[]> records = consumer.poll(pollTimeoutMs);
      lastFetchTime = (System.currentTimeMillis() - tempTime);
      log.debug("Time taken to fetch : " + (lastFetchTime / 1000) + " seconds");
      log.debug("The number of ConsumerRecord returned is : " + records.count());
      int skipped = 0;
      totalFetchTime += lastFetchTime;
      messageIter = records.iterator();
      Iterator<ConsumerRecord<byte[], byte[]>> messageIter2 = records.iterator();
      ConsumerRecord<byte[], byte[]> record;

      while (messageIter2.hasNext()) {
        record = messageIter2.next();
        if (record.offset() < currentOffset) {
          skipped++;
        } else {
          log.debug("Skipped offsets till : " + record.offset());
          break;
        }
      }
      log.debug("Number of offsets to be skipped: " + skipped);
      while (skipped != 0) {
        ConsumerRecord<byte[], byte[]> skippedMessage = messageIter.next();
        log.debug("Skipping offset : " + skippedMessage.offset());
        skipped--;
      }

      if (!messageIter.hasNext()) {
        System.out.println("No more data left to process. Returning false");
        messageIter = null;
        return false;
      }

      return true;
    } catch (Exception e) {
      log.info("Exception generated during fetch");
      e.printStackTrace();
      return false;
    }

  }

  /**
   * Closes this context
   *
   * @throws IOException
   */
  public void close() {
    if (consumer != null) {
      consumer.close();
    }
  }

  /**
   * Returns the total bytes that will be fetched. This is calculated by
   * taking the diffs of the offsets
   *
   * @return
   */
  public long getTotalBytes() {
    return (lastOffset > beginOffset) ? lastOffset - beginOffset : 0;
  }

  /**
   * Returns the total bytes that have been fetched so far
   *
   * @return
   */
  public long getReadBytes() {
    return currentOffset - beginOffset;
  }

  /**
   * Returns the number of events that have been read r
   *
   * @return
   */
  public long getCount() {
    return currentCount;
  }

  /**
   * Returns the fetch time of the last fetch in ms
   *
   * @return
   */
  public long getFetchTime() {
    return lastFetchTime;
  }

  /**
   * Returns the totalFetchTime in ms
   *
   * @return
   */
  public long getTotalFetchTime() {
    return totalFetchTime;
  }

  public static class MalformedMessageException extends Exception {
    public MalformedMessageException(Throwable cause) {
      super(cause);
    }

    public MalformedMessageException(String error) {
      super(error);
    }

    public MalformedMessageException(String error, Throwable cause) {
      super(error, cause);
    }

  }
}
