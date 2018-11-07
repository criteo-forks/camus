package com.linkedin.camus.etl.kafka.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.*;

import com.linkedin.camus.etl.IEtlKey;


/**
 * The key for the mapreduce job to pull kafka. Contains offsets and the
 * checksum.
 */
public class EtlKey implements WritableComparable<EtlKey>, IEtlKey {
  public static final Text SERVER = new Text("server");
  public static final Text SERVICE = new Text("service");
  private static final Text MESSAGE_SIZE_KEY = new Text("message.size");

  private int partition = 0;
  private long beginOffset = 0;
  private long offset = 0;
  private long checksum = 0;
  private String topic = "";
  private long time = 0;
  private String server = "";
  private String service = "";
  private MapWritable partitionMap = new MapWritable();

  /**
   * dummy empty constructor
   */
  public EtlKey() {
    this("dummy", 0, 0, 0, 0);
  }

  public EtlKey(EtlKey other) {

    this.partition = other.partition;
    this.beginOffset = other.beginOffset;
    this.offset = other.offset;
    this.checksum = other.checksum;
    this.topic = other.topic;
    this.time = other.time;
    this.server = other.server;
    this.service = other.service;
    this.partitionMap = new MapWritable(other.partitionMap);
  }

  public EtlKey(String topic, int partition) {
    this.set(topic, partition, 0, 0, 0);
  }

  public EtlKey(String topic, int partition, long beginOffset, long offset) {
    this.set(topic, partition, beginOffset, offset, 0);
  }

  public EtlKey(String topic, int partition, long beginOffset, long offset, long checksum) {
    this.set(topic, partition, beginOffset, offset, checksum);
  }

  public void set(String topic, int partition, long beginOffset, long offset, long checksum) {
    this.partition = partition;
    this.beginOffset = beginOffset;
    this.offset = offset;
    this.checksum = checksum;
    this.topic = topic;
    this.time = System.currentTimeMillis(); // if event can't be decoded,
    // this time will be used for
    // debugging.
  }

  public void clear() {
    partition = 0;
    beginOffset = 0;
    offset = 0;
    checksum = 0;
    topic = "";
    time = 0;
    server = "";
    service = "";
    partitionMap = new MapWritable();
  }

  public String getServer() {
    return partitionMap.get(SERVER).toString();
  }

  public void setServer(String newServer) {
    partitionMap.put(SERVER, new Text(newServer));
  }

  public String getService() {
    return partitionMap.get(SERVICE).toString();
  }

  public void setService(String newService) {
    partitionMap.put(SERVICE, new Text(newService));
  }

  public long getTime() {
    return time;
  }

  public void setTime(long time) {
    this.time = time;
  }

  public String getTopic() {
    return topic;
  }

  public int getPartition() {
    return this.partition;
  }

  public long getBeginOffset() {
    return this.beginOffset;
  }

  public void setOffset(long offset) {
    this.offset = offset;
  }

  public long getOffset() {
    return this.offset;
  }

  public long getChecksum() {
    return this.checksum;
  }

  @Override
  public long getMessageSize() {
    if (this.partitionMap.containsKey(MESSAGE_SIZE_KEY))
      return ((LongWritable) this.partitionMap.get(MESSAGE_SIZE_KEY)).get();
    else
      return 1024; //default estimated size
  }

  public void setMessageSize(long messageSize) {
    put(MESSAGE_SIZE_KEY, new LongWritable(messageSize));
  }

  public void put(Writable key, Writable value) {
    this.partitionMap.put(key, value);
  }

  public void addAllPartitionMap(MapWritable partitionMap) {
    this.partitionMap.putAll(partitionMap);
  }

  public MapWritable getPartitionMap() {
    return partitionMap;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    UTF8.readString(in); // deprecated, was leaderId
    this.partition = in.readInt();
    this.beginOffset = in.readLong();
    this.offset = in.readLong();
    this.checksum = in.readLong();
    this.topic = in.readUTF();
    this.time = in.readLong();
    this.server = in.readUTF(); // left for legacy
    this.service = in.readUTF(); // left for legacy
    this.partitionMap = new MapWritable();
    try {
      this.partitionMap.readFields(in);
    } catch (IOException e) {
      this.setServer(this.server);
      this.setService(this.service);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    UTF8.writeString(out, "");
    out.writeInt(this.partition);
    out.writeLong(this.beginOffset);
    out.writeLong(this.offset);
    out.writeLong(this.checksum);
    out.writeUTF(this.topic);
    out.writeLong(this.time);
    out.writeUTF(this.server); // left for legacy
    out.writeUTF(this.service); // left for legacy
    this.partitionMap.write(out);
  }

  @Override
  public int compareTo(EtlKey o) {
    if (partition != o.partition) {
      return partition = o.partition;
    } else {
      if (offset > o.offset) {
        return 1;
      } else if (offset < o.offset) {
        return -1;
      } else {
        return Long.compare(checksum, o.checksum);
      }
    }
  }

  @Override
  public String toString() {
    return String.format("topic=%s partition=%d beginOffset=%d offset=%d msgSize=%d checksum=%d time=%d",
            topic, partition, beginOffset, offset, getMessageSize(), checksum, time);
  }
}
