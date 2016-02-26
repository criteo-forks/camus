package com.linkedin.camus.etl.kafka.common;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import kafka.api.OffsetRequest;
import kafka.api.PartitionFetchInfo;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.OffsetMetadataAndError$;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchRequest;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.MessageAndOffset;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import com.linkedin.camus.etl.kafka.CamusJob;
import com.linkedin.camus.etl.kafka.mapred.EtlInputFormat;


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
  private SimpleConsumer simpleConsumer = null;

  private long beginOffset;
  private long currentOffset;
  private long lastOffset;
  private long currentCount;

  private TaskAttemptContext context;

  private Iterator<MessageAndOffset> messageIter = null;

  private long totalFetchTime = 0;
  private long lastFetchTime = 0;

  private int fetchBufferSize;

  /**
   * Construct using the json representation of the kafka request
   */
  public KafkaReader(EtlInputFormat inputFormat, TaskAttemptContext context, EtlRequest request, 
                     int clientTimeout, int fetchBufferSize)
      throws Exception {
    this.fetchBufferSize = fetchBufferSize;
    this.context = context;

    log.info("bufferSize=" + fetchBufferSize);
    log.info("timeout=" + clientTimeout);

    // Create the kafka request from the json

    kafkaRequest = request;

    beginOffset = request.getOffset();
    currentOffset = request.getOffset();
    lastOffset = request.getLastOffset();
    currentCount = 0;
    totalFetchTime = 0;

    // read data from queue

    URI uri = kafkaRequest.getURI();
    simpleConsumer =
        inputFormat.createSimpleConsumer(context, uri.getHost(), uri.getPort());
    log.info("Connected to leader " + uri + " beginning reading at offset " + beginOffset + " latest offset="
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
   * @throws IOException
   */
  public boolean getNext(EtlKey key, BytesWritable payload, BytesWritable pKey) throws IOException {
    if (hasNext()) {

      MessageAndOffset msgAndOffset = messageIter.next();
      Message message = msgAndOffset.message();

      ByteBuffer buf = message.payload();
      int origSize = buf.remaining();
      byte[] bytes = new byte[origSize];
      buf.get(bytes, buf.position(), origSize);
      payload.set(bytes, 0, origSize);

      buf = message.key();
      if (buf != null) {
        origSize = buf.remaining();
        bytes = new byte[origSize];
        buf.get(bytes, buf.position(), origSize);
        pKey.set(bytes, 0, origSize);
      }

      key.clear();
      key.set(kafkaRequest.getTopic(), kafkaRequest.getLeaderId(), kafkaRequest.getPartition(), currentOffset,
          msgAndOffset.offset() + 1, message.checksum());

      key.setMessageSize(msgAndOffset.message().size());

      currentOffset = msgAndOffset.offset() + 1; // increase offset
      currentCount++; // increase count

      return true;
    } else {
      return false;
    }
  }

  /**
   * Retry failed FetchRequest for current topic and partition if the offset of the request is out of range in the past.
   * @param fetchResponse the failed FetchResponse
   * @param topicAndPartition the current topic and parititon
   * @return either the given FetchResponse if there is error or the new one
   */
  public FetchResponse retryFetchRequestOnOutOfRange(FetchResponse fetchResponse, TopicAndPartition topicAndPartition){
    // retry only if the fetchresponse replied offset out of range
    if(fetchResponse.hasError() &&
            fetchResponse.errorCode(topicAndPartition.topic(), topicAndPartition.partition()) == ErrorMapping.OffsetOutOfRangeCode()){
      // create a new OffsetRequest
      Map<TopicAndPartition, PartitionOffsetRequestInfo> offsetRequestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
      // Only fetch 1 offset for the current topic + partition
      offsetRequestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(kafka.api.OffsetRequest.EarliestTime(), 1));
      // Create offset request
      kafka.javaapi.OffsetRequest offsetRequest =
              new kafka.javaapi.OffsetRequest(offsetRequestInfo, OffsetRequest.CurrentVersion(), CamusJob.getKafkaClientName(context));
      OffsetResponse offsetsBefore = simpleConsumer.getOffsetsBefore(offsetRequest);
      if(!offsetsBefore.hasError()){
        long[] offsets = offsetsBefore.offsets(topicAndPartition.topic(), topicAndPartition.partition());
        // As we fetched only 1 offset we have only 1 offset in the offsets array
        // If we are out of range in the past retry the FetchRequest with the new earliest offset
        if(currentOffset < offsets[0]){
          PartitionFetchInfo partitionFetchInfo = new PartitionFetchInfo(offsets[0], fetchBufferSize);
          HashMap<TopicAndPartition, PartitionFetchInfo> fetchInfo = new HashMap<TopicAndPartition, PartitionFetchInfo>();
          fetchInfo.put(topicAndPartition, partitionFetchInfo);
          FetchRequest fetchRequest = new FetchRequest(CamusJob.getKafkaFetchRequestCorrelationId(context), CamusJob.getKafkaClientName(context),
                  CamusJob.getKafkaFetchRequestMaxWait(context), CamusJob.getKafkaFetchRequestMinBytes(context), fetchInfo);
          return simpleConsumer.fetch(fetchRequest);
        }
      }
    }
    return fetchResponse;
  }

  /**
   * Creates a fetch request.
   *
   * @return false if there's no more fetches
   * @throws IOException
   */

  public boolean fetch() throws IOException {
    if (currentOffset >= lastOffset) {
      return false;
    }
    long tempTime = System.currentTimeMillis();
    TopicAndPartition topicAndPartition = new TopicAndPartition(kafkaRequest.getTopic(), kafkaRequest.getPartition());
    log.debug("\nAsking for offset : " + (currentOffset));
    PartitionFetchInfo partitionFetchInfo = new PartitionFetchInfo(currentOffset, fetchBufferSize);

    HashMap<TopicAndPartition, PartitionFetchInfo> fetchInfo = new HashMap<TopicAndPartition, PartitionFetchInfo>();
    fetchInfo.put(topicAndPartition, partitionFetchInfo);

    FetchRequest fetchRequest =
        new FetchRequest(CamusJob.getKafkaFetchRequestCorrelationId(context), CamusJob.getKafkaClientName(context),
            CamusJob.getKafkaFetchRequestMaxWait(context), CamusJob.getKafkaFetchRequestMinBytes(context), fetchInfo);

    FetchResponse fetchResponse = null;
    try {
      fetchResponse = simpleConsumer.fetch(fetchRequest);

      if(fetchResponse.hasError()) {
        retryFetchRequestOnOutOfRange(fetchResponse, topicAndPartition);
      }

      if (fetchResponse.hasError()) {
        log.info("Error encountered during a fetch request from Kafka");
        log.info("Error Code generated : "
            + fetchResponse.errorCode(kafkaRequest.getTopic(), kafkaRequest.getPartition()));
        return false;
      } else {
        ByteBufferMessageSet messageBuffer =
            fetchResponse.messageSet(kafkaRequest.getTopic(), kafkaRequest.getPartition());
        lastFetchTime = (System.currentTimeMillis() - tempTime);
        log.debug("Time taken to fetch : " + (lastFetchTime / 1000) + " seconds");
        log.debug("The size of the ByteBufferMessageSet returned is : " + messageBuffer.sizeInBytes());
        int skipped = 0;
        totalFetchTime += lastFetchTime;
        messageIter = messageBuffer.iterator();
        //boolean flag = false;
        Iterator<MessageAndOffset> messageIter2 = messageBuffer.iterator();
        MessageAndOffset message = null;
        while (messageIter2.hasNext()) {
          message = messageIter2.next();
          if (message.offset() < currentOffset) {
            //flag = true;
            skipped++;
          } else {
            log.debug("Skipped offsets till : " + message.offset());
            break;
          }
        }
        log.debug("Number of offsets to be skipped: " + skipped);
        while (skipped != 0) {
          MessageAndOffset skippedMessage = messageIter.next();
          log.debug("Skipping offset : " + skippedMessage.offset());
          skipped--;
        }

        if (!messageIter.hasNext()) {
          System.out.println("No more data left to process. Returning false");
          messageIter = null;
          return false;
        }

        return true;
      }
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
  public void close() throws IOException {
    if (simpleConsumer != null) {
      simpleConsumer.close();
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
}
