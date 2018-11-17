package com.linkedin.camus.etl.kafka.common;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.camus.etl.kafka.CamusJob;
import com.linkedin.camus.etl.kafka.mapred.EtlInputFormat;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


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

  private final EtlInputFormat inputFormat;
  private TaskAttemptContext context;

  private Iterator<ConsumerRecord<byte[], byte[]>> messageIter = null;

  private long totalFetchTime = 0;
  private long lastFetchTime = 0;
  private final long pollTimeoutMs;
  private final TopicPartition topicPartition;

  public static final Pattern BAD_MASSAGE_PATTERN = Pattern.compile("Error deserializing key/value for partition (?<topic>.*)-(?<partition>[0-9]*) at offset (?<offset>[0-9]*)");


  /**
   * Construct using the json representation of the kafka request
   */
  public KafkaReader(EtlInputFormat inputFormat, TaskAttemptContext context, EtlRequest request) {
    this.inputFormat = inputFormat;
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
    topicPartition = new TopicPartition(kafkaRequest.getTopic(), kafkaRequest.getPartition());
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
      key.set(kafkaRequest.getTopic(), kafkaRequest.getPartition(), currentOffset, record.offset() + 1, record.checksum());

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
      ConsumerRecords<byte[], byte[]> records = safepoll(pollTimeoutMs);
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
    } catch (SerializationException e) {
      long faultyRecordOffset = consumer.position(topicPartition);
      log.info("Exception deserializing record with offset " + faultyRecordOffset + " for partition " + topicPartition);
      consumer.seek(topicPartition, faultyRecordOffset + 1);
      return true;
    } catch (Exception e) {
      log.info("Exception generated during fetch");
      e.printStackTrace();
      return false;
    }

  }

  @VisibleForTesting
  public static Map.Entry<TopicPartition,Long> parseErrorString(Pattern badMassagePattern, String badMassageErrorString) {
    Matcher badMessageMatcher = badMassagePattern.matcher(badMassageErrorString);
    if ( badMessageMatcher.find() )
    {
      String badMessageTopic = badMessageMatcher.group("topic");
      Integer badMessagePartition = Integer.valueOf(badMessageMatcher.group("partition"));
      Long badMessageOffset = Long.valueOf(badMessageMatcher.group("offset"));
      return new AbstractMap.SimpleEntry<TopicPartition, Long>(new TopicPartition(badMessageTopic,badMessagePartition), badMessageOffset);
    }
    else {
      log.error("Got bad message bug can't parse it:"+badMassageErrorString);
    }
    return null;
  }

  private ConsumerRecords<byte[], byte[]> safepoll(long pollTimeoutMs) {
    try {
      return consumer.poll(pollTimeoutMs);
    } catch (SerializationException e) {
      return handleBadMessage(e, pollTimeoutMs);
    }
  }

  private ConsumerRecords<byte[], byte[]> handleBadMessage(SerializationException serializationException, long poll_timeout) {
    //This exception happens only if we got bad message
    log.warn("Got bad message: " + serializationException.getLocalizedMessage());

    Map.Entry<TopicPartition, Long> badMessageOffset = parseErrorString(BAD_MASSAGE_PATTERN, serializationException.getMessage());
    //If we can't parse error message - there is nothing we can do
    if (badMessageOffset == null) {
      throw serializationException;
    }
    //Amount of messages from last comitted up to failed
    Long currentPosition = consumer.position(badMessageOffset.getKey());
    Long messagesToRead = badMessageOffset.getValue() - currentPosition;
    log.warn("Current position is " + messagesToRead + " messages from bad");

    // this hack to extract the offset of bad messages is not needed anymore in kafka 1.1.1, but does not cost much
    // as we are creating a one time consumer only in case of missed messages.
    if (messagesToRead >= 1) {
      //Ok, lets try to download only good messages.
      //Lets create consumer out of our group (avoid rebalance)
      Properties onetimeConsumerConfig = inputFormat.getKafkaProperties(context);
      onetimeConsumerConfig.put(ConsumerConfig.CLIENT_ID_CONFIG, "OneTimeConsumer-" + onetimeConsumerConfig.hashCode());
      onetimeConsumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "OneTimeConsumer-" + onetimeConsumerConfig.hashCode());
      //Lowering fetch buffer
      onetimeConsumerConfig.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 5);
      onetimeConsumerConfig.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, 20);
      onetimeConsumerConfig.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, messagesToRead.intValue());

      KafkaConsumer<byte[], byte[]> onetimeconsumer = new KafkaConsumer<>(onetimeConsumerConfig);
      onetimeconsumer.assign(Collections.singletonList(badMessageOffset.getKey()));
      onetimeconsumer.seek(badMessageOffset.getKey(), currentPosition);

      //Slide original consumer to next position
      consumer.seek(badMessageOffset.getKey(), badMessageOffset.getValue() + 1);

      //Return all messages that was not enough
      List<ConsumerRecord<byte[], byte[]>> buffer = new ArrayList<>(messagesToRead.intValue());
      try {
        while (buffer.size() < messagesToRead) {
          ConsumerRecords<byte[], byte[]> consumerRecords = onetimeconsumer.poll(poll_timeout * 3);
          buffer.addAll(consumerRecords.records(badMessageOffset.getKey()));
          log.trace("Downloaded " + consumerRecords.count() + " messages");
        }
        onetimeconsumer.close();
      } catch (Exception bomb) {
        log.error("We triggered bomb, saving messages failed", bomb);
        onetimeconsumer.close();
      }
      log.info("We saved " + buffer.size() + " messages from " + messagesToRead.intValue());
      return new ConsumerRecords<>(Collections.singletonMap(badMessageOffset.getKey(), buffer));
    }
    else {
      consumer.seek(badMessageOffset.getKey(),badMessageOffset.getValue()+1);
      return safepoll(pollTimeoutMs);
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
