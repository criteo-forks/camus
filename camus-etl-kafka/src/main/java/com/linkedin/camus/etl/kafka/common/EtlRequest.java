package com.linkedin.camus.etl.kafka.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.Properties;

import com.linkedin.camus.etl.kafka.mapred.EtlInputFormat;

import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import com.linkedin.camus.workallocater.CamusRequest;


/**
 * A class that represents the kafka pull request.
 * 
 * The class is a container for topic, leaderId, partition, uri and offset. It is
 * used in reading and writing the sequence files used for the extraction job.
 * 
 * @author Richard Park
 */
public class EtlRequest implements CamusRequest {

  private JobContext context = null;
  public static final long DEFAULT_OFFSET = 0;

  private String topic = "";
  private String leaderId = "";
  private int partition = 0;

  private long offset = DEFAULT_OFFSET;
  private long latestOffset = -1;
  private long earliestOffset = -2;

  private long avgMsgSize = 1024;

  public EtlRequest() {
  }

  public EtlRequest(EtlRequest other) {
    this.topic = other.topic;
    this.leaderId = other.leaderId;
    this.partition = other.partition;
    this.offset = other.offset;
    this.latestOffset = other.latestOffset;
    this.earliestOffset = other.earliestOffset;
    this.avgMsgSize = other.avgMsgSize;
  }

  /* (non-Javadoc)
   * @see com.linkedin.camus.etl.kafka.common.CamusRequest#setLatestOffset(long)
   */
  @Override
  public void setLatestOffset(long latestOffset) {
    this.latestOffset = latestOffset;
  }

  /* (non-Javadoc)
   * @see com.linkedin.camus.etl.kafka.common.CamusRequest#setEarliestOffset(long)
   */
  @Override
  public void setEarliestOffset(long earliestOffset) {
    this.earliestOffset = earliestOffset;
  }

  public void setAvgMsgSize(long size) {
    this.avgMsgSize = size;
  }

  /**
   * Constructor for the KafkaETLRequest with the offset to -1.
   *
   * @param topic
   *            The topic name
   *  @param leaderId
   *            The leader broker for this topic and partition
   * @param partition
   *            The partition to pull
   */
  public EtlRequest(JobContext context, String topic, String leaderId, int partition) {
    this(context, topic, leaderId, partition, DEFAULT_OFFSET);
  }

  /**
   * Constructor for the full kafka pull job. Neither the brokerUri nor offset
   * are used to ensure uniqueness.
   * 
   * @param topic
   *            The topic name
   * @param partition
   *            The partition to pull
   * @param offset
   */
  public EtlRequest(JobContext context, String topic, String leaderId, int partition, long offset) {
    this.context = context;
    this.topic = topic;
    this.leaderId = leaderId;
    this.partition = partition;
    setOffset(offset);
  }

  /* (non-Javadoc)
   * @see com.linkedin.camus.etl.kafka.common.CamusRequest#setOffset(long)
   */
  @Override
  public void setOffset(long offset) {
    this.offset = offset;
  }

  /**
   * Retrieve the broker node id.
   *
   * @return
   */
  public String getLeaderId() {
    return this.leaderId;
  }

  /* (non-Javadoc)
   * @see com.linkedin.camus.etl.kafka.common.CamusRequest#getTopic()
   */
  @Override
  public String getTopic() {
    return this.topic;
  }

  /* (non-Javadoc)
   * @see com.linkedin.camus.etl.kafka.common.CamusRequest#getPartition()
   */
  @Override
  public int getPartition() {
    return this.partition;
  }

  /* (non-Javadoc)
   * @see com.linkedin.camus.etl.kafka.common.CamusRequest#getOffset()
   */
  @Override
  public long getOffset() {
    return this.offset;
  }

  public void setLeaderId(String leaderId) {
    this.leaderId = leaderId;
  }

  /* (non-Javadoc)
   * @see com.linkedin.camus.etl.kafka.common.CamusRequest#isValidOffset()
   */
  @Override
  public boolean isValidOffset() {
    return this.offset >= 0;
  }

  @Override
  public String toString() {
    return topic + "\tleader:" + leaderId + "\tpartition:" + partition + "\tearliest_offset:" + getEarliestOffset() +
            "\toffset:" + offset + "\tlatest_offset:" + getLastOffset() + "\tavg_msg_size:" + avgMsgSize +
            "\testimated_size:" + estimateDataSize();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (!(o instanceof EtlRequest))
      return false;

    EtlRequest that = (EtlRequest) o;

    if (partition != that.partition)
      return false;
    if (!topic.equals(that.topic))
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = topic.hashCode();
    result = 31 * result + partition;
    return result;
  }

  /**
   * Returns the copy of KafkaETLRequest
   */
  @Override
  public CamusRequest clone() {
    return new EtlRequest(context, topic, leaderId, partition, offset);
  }

  /* (non-Javadoc)
   * @see com.linkedin.camus.etl.kafka.common.CamusRequest#getEarliestOffset()
   */
  @Override
  public long getEarliestOffset() {
    if (this.earliestOffset == -2) {
      Properties properties = EtlInputFormat.getKafkaProperties(context);
      try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(properties)) {
        TopicPartition tp = new TopicPartition(topic, partition);
        this.earliestOffset = consumer.beginningOffsets(Collections.singletonList(tp)).get(tp);
      }
    }

    return this.earliestOffset;
  }

  /* (non-Javadoc)
   * @see com.linkedin.camus.etl.kafka.common.CamusRequest#getLastOffset()
   */
  @Override
  public long getLastOffset() {
    if (this.latestOffset == -1) {
      Properties properties = EtlInputFormat.getKafkaProperties(context);
      try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(properties)) {
        TopicPartition tp = new TopicPartition(topic, partition);
        this.latestOffset = consumer.endOffsets(Collections.singletonList(tp)).get(tp);
      }
    }

    return this.latestOffset;
  }

  /* (non-Javadoc)
   * @see com.linkedin.camus.etl.kafka.common.CamusRequest#estimateDataSize()
   */
  @Override
  public long estimateDataSize() {
    long endOffset = getLastOffset();
    return (endOffset - offset) * avgMsgSize;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    topic = UTF8.readString(in);
    leaderId = UTF8.readString(in);
    UTF8.readString(in); // deprecated, was leader URI
    partition = in.readInt();
    offset = in.readLong();
    latestOffset = in.readLong();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    UTF8.writeString(out, topic);
    UTF8.writeString(out, leaderId);
    UTF8.writeString(out, "");  // deprecated, was leader URI
    out.writeInt(partition);
    out.writeLong(offset);
    out.writeLong(latestOffset);
  }
}
