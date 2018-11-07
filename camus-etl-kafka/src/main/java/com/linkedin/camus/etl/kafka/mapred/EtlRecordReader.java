package com.linkedin.camus.etl.kafka.mapred;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.coders.MessageDecoder;
import com.linkedin.camus.etl.kafka.CamusJob;
import com.linkedin.camus.etl.kafka.coders.MessageDecoderFactory;
import com.linkedin.camus.etl.kafka.common.EtlKey;
import com.linkedin.camus.etl.kafka.common.EtlRequest;
import com.linkedin.camus.etl.kafka.common.ExceptionWritable;
import com.linkedin.camus.etl.kafka.common.KafkaReader;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicReference;


public class EtlRecordReader extends RecordReader<EtlKey, CamusWrapper> {
  private static final String PRINT_MAX_DECODER_EXCEPTIONS = "max.decoder.exceptions.to.print";
  private static final String DEFAULT_SERVER = "server";
  private static final String DEFAULT_SERVICE = "service";
  private TaskAttemptContext context;

  private EtlInputFormat inputFormat;
  private Mapper<EtlKey, Writable, EtlKey, Writable>.Context mapperContext;
  private KafkaReader reader;

  private long totalBytes;
  private long readBytes = 0;

  private boolean skipSchemaErrors = false;
  private MessageDecoder decoder;
  private final BytesWritable msgValue = new BytesWritable();
  private final BytesWritable msgKey = new BytesWritable();
  private final EtlKey key = new EtlKey();
  private CamusWrapper value;

  private int maxPullHours = 0;
  private int exceptionCount = 0;
  private long maxPullTime = 0;
  private long endTimeStamp = 0;
  private long curTimeStamp = 0;
  private HashSet<String> ignoreServerServiceList = null;

  private String statusMsg = "";

  EtlSplit split;
  private static Logger log = Logger.getLogger(EtlRecordReader.class);

  /**
   * Record reader to fetch directly from Kafka
   *
   * @param split
   * @throws IOException
   * @throws InterruptedException
   */
  public EtlRecordReader(EtlInputFormat inputFormat, InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    this.inputFormat = inputFormat;
    initialize(split, context);
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Override
  public void initialize(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    // For class path debugging
    log.info("classpath: " + System.getProperty("java.class.path"));
    ClassLoader loader = EtlRecordReader.class.getClassLoader();
    log.info("PWD: " + System.getProperty("user.dir"));
    log.info("classloader: " + loader.getClass());
    log.info("org.apache.avro.Schema: " + loader.getResource("org/apache/avro/Schema.class"));

    this.split = (EtlSplit) split;
    this.context = context;

    if (context instanceof Mapper.Context) {
      mapperContext = (Mapper.Context) context;
    }

    this.skipSchemaErrors = EtlInputFormat.getEtlIgnoreSchemaErrors(context);

    if (EtlInputFormat.getKafkaMaxPullHrs(context) != -1) {
      this.maxPullHours = EtlInputFormat.getKafkaMaxPullHrs(context);
    } else {
      this.endTimeStamp = Long.MAX_VALUE;
    }

    if (EtlInputFormat.getKafkaMaxPullMinutesPerTask(context) != -1) {
      DateTime now = new DateTime();
      this.maxPullTime = now.plusMinutes(EtlInputFormat.getKafkaMaxPullMinutesPerTask(context)).getMillis();
    } else {
      this.maxPullTime = Long.MAX_VALUE;
    }

    ignoreServerServiceList = new HashSet<String>();
    for (String ignoreServerServiceTopic : EtlInputFormat.getEtlAuditIgnoreServiceTopicList(context)) {
      ignoreServerServiceList.add(ignoreServerServiceTopic);
    }

    this.totalBytes = this.split.getLength();
  }

  @Override
  public synchronized void close() throws IOException {
    if (reader != null) {
      reader.close();
    }
  }

  protected CamusWrapper getWrappedRecord(String topicName, byte[] payload) throws IOException {
    CamusWrapper r = null;
    try {
      r = decoder.decode(payload);
    } catch (Exception e) {
      if (!skipSchemaErrors) {
        throw new IOException(e);
      }
    }
    return r;
  }

  private static byte[] getBytes(BytesWritable val) {
    byte[] buffer = val.getBytes();

    /*
     * FIXME: remove the following part once the below jira is fixed
     * https://issues.apache.org/jira/browse/HADOOP-6298
     */
    long len = val.getLength();
    byte[] bytes = buffer;
    if (len < buffer.length) {
      bytes = new byte[(int) len];
      System.arraycopy(buffer, 0, bytes, 0, (int) len);
    }

    return bytes;
  }

  @Override
  public float getProgress() throws IOException {
    if (getPos() == 0) {
      return 0f;
    }

    if (getPos() >= totalBytes) {
      return 1f;
    }
    return (float) ((double) getPos() / totalBytes);
  }

  private long getPos() throws IOException {
    return readBytes;
  }

  @Override
  public EtlKey getCurrentKey() throws IOException, InterruptedException {
    return key;
  }

  @Override
  public CamusWrapper getCurrentValue() throws IOException, InterruptedException {
    return value;
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {

    if (System.currentTimeMillis() > maxPullTime) {
      String maxMsg = "at " + new DateTime(curTimeStamp).toString();
      log.info("Kafka pull time limit reached");
      statusMsg += " max read " + maxMsg;
      context.setStatus(statusMsg);
      log.info(key.getTopic() + " max read " + maxMsg);
      closeReader();

      mapperContext.write(key, new ExceptionWritable("Topic not fully pulled, max task time reached" + maxMsg));

      EtlRequest request = null;

      while ((request = (EtlRequest) split.popRequest()) != null) {
        key.set(request.getTopic(), request.getLeaderId(), request.getPartition(), request.getOffset(),
            request.getOffset(), 0);
        //Set lag on partitions with request
//        mapperContext.getCounter("offsetlag_per_partition", Integer.toString(key.getPartition())).increment(request.getLastOffset() - request.getOffset());
//        mapperContext.getCounter("offsetlag_per_brokerid", key.getLeaderId()).increment(request.getLastOffset() - request.getOffset());
        mapperContext.write(key, new ExceptionWritable("Topic not fully pulled, max task time reached"));
      }

      return false;
    }

    while (true) {
      try {
        if (reader == null || !reader.hasNext()) {
          EtlRequest request = (EtlRequest) split.popRequest();
          if (request == null) {
            return false;
          }

          if (maxPullHours > 0) {
            endTimeStamp = 0;
          }

          key.set(request.getTopic(), request.getLeaderId(), request.getPartition(), request.getOffset(),
              request.getOffset(), 0);
          value = null;
          log.info("\n\ntopic:" + request.getTopic() + " partition:" + request.getPartition() + " beginOffset:"
              + request.getOffset() + " estimatedLastOffset:" + request.getLastOffset());

          statusMsg += statusMsg.length() > 0 ? "; " : "";
          statusMsg += request.getTopic() + ":" + request.getLeaderId() + ":" + request.getPartition();
          context.setStatus(statusMsg);

          if (reader != null) {
            closeReader();
          }
          reader = new KafkaReader(inputFormat, context, request);

          decoder = MessageDecoderFactory.createMessageDecoder(context, request.getTopic());
          //          mapperContext.getCounter("offsetlag_per_partition", Integer.toString(key.getPartition())).setValue(request.getLastOffset() - request.getOffset());
          //          mapperContext.getCounter("offsetlag_per_brokerid", key.getLeaderId()).setValue(request.getLastOffset() - request.getOffset());
        }
        int count = 0;
        long messagebytes = 0;
        AtomicReference<ConsumerRecord<byte[], byte[]>> messageRef = new AtomicReference<>();
        while (reader.getNext(key, msgValue, msgKey, messageRef)) {
          readBytes += key.getMessageSize();
          count++;
          messagebytes += msgValue.getLength();
          context.progress();
          mapperContext.getCounter("total", "event-count").increment(1);
          byte[] bytes = getBytes(msgValue);

          long tempTime = System.currentTimeMillis();
          CamusWrapper wrapper;
          try {
            wrapper = getWrappedRecord(key.getTopic(), bytes);
          } catch (Exception e) {
            context.getCounter("total", "decoded-failed").increment(1L);
            if (exceptionCount < getMaximumDecoderExceptionsToPrint(context)) {
              mapperContext.write(key, new ExceptionWritable(e));
              exceptionCount++;
            } else if (exceptionCount == getMaximumDecoderExceptionsToPrint(context)) {
              exceptionCount = Integer.MAX_VALUE; //Any random value
              log.info("The same exception has occured for more than " + getMaximumDecoderExceptionsToPrint(context)
                  + " records. All further exceptions will not be printed");
            }
            continue;
          }

          if (wrapper == null) {
            mapperContext.write(key, new ExceptionWritable(new RuntimeException("null record")));
            continue;
          }

          curTimeStamp = wrapper.getTimestamp();
          try {
            key.setTime(curTimeStamp);
            key.addAllPartitionMap(wrapper.getPartitionMap());
            setServerService();
          } catch (Exception e) {
            mapperContext.write(key, new ExceptionWritable(e));
            continue;
          }

          if (endTimeStamp == 0) {
            DateTime time = new DateTime(curTimeStamp);
            statusMsg = "begin read at " + time.toString() + " " + statusMsg;
            context.setStatus(statusMsg);
            log.info(key.getTopic() + " begin read at " + time.toString());
            endTimeStamp = (time.plusHours(this.maxPullHours)).getMillis();
          } else if (curTimeStamp > endTimeStamp) {
            String maxMsg = "at " + new DateTime(curTimeStamp).toString();
            log.info("Kafka Max history hours reached");
            mapperContext.write(key, new ExceptionWritable("Topic not fully pulled, max partition hours reached"
                + maxMsg));
            statusMsg = "max read " + maxMsg + " " + statusMsg;
            context.setStatus(statusMsg);
            log.info(key.getTopic() + " max read " + maxMsg);
            //Here lag have to be decreased on  "count" messages for key
            //            mapperContext.getCounter("offsetlag_per_partition", Integer.toString(key.getPartition())).increment(-count);
            //            mapperContext.getCounter("offsetlag_per_brokerid", key.getLeaderId()).increment(-count);

            closeReader();
          }

          long secondTime = System.currentTimeMillis();
          value = wrapper;
          long decodeTime = ((secondTime - tempTime));
          mapperContext.getCounter("total", "data-read").increment(messagebytes);

          //          mapperContext.getCounter("messages_per_brokerid", key.getLeaderId()).increment(count);
          //          mapperContext.getCounter("messages_per_partition", Integer.toString(key.getPartition())).increment(count);

          //          mapperContext.getCounter("bytes_per_partition", Integer.toString(key.getPartition())).increment(messagebytes);
          //          mapperContext.getCounter("bytes_per_brokerid", key.getLeaderId()).increment(messagebytes);

          //If we reach this part, then no lag on current key
          //          mapperContext.getCounter("offsetlag_per_partition", Integer.toString(key.getPartition())).setValue(0);
          //          mapperContext.getCounter("offsetlag_per_brokerid", key.getLeaderId()).setValue(0);

          //          mapperContext.getCounter("timestamp_per_partition", Integer.toString(key.getPartition())).setValue(curTimeStamp/1000);

          mapperContext.getCounter("total", "decode-time(ms)").increment(decodeTime);
          return true;
        }
        log.info("Records read : " + count);
        count = 0;
        reader = null;
      } catch (KafkaReader.MalformedMessageException mme){
        // Hack to fix the problem when we run into an empty message.
        mapperContext.write(key, new ExceptionWritable(mme));
        // Without reset reader, will continue to read messages out of it.
      } catch (Throwable t) {
        Exception e = new Exception(t.getLocalizedMessage(), t);
        e.setStackTrace(t.getStackTrace());
        mapperContext.write(key, new ExceptionWritable(e));
        reader = null;
      }
    }
  }

  private void closeReader() throws IOException {
    if (reader != null) {
      mapperContext.getCounter("total", "request-time(ms)").increment(reader.getTotalFetchTime());
      mapperContext.getCounter("total", "reader-closed").increment(1);
      try {
        reader.close();
      } catch (Exception e) {
        // not much to do here but skip the task
      } finally {
        reader = null;
      }
    }
  }

  public void setServerService() {
    if (ignoreServerServiceList.contains(key.getTopic()) || ignoreServerServiceList.contains("all")) {
      key.setServer(DEFAULT_SERVER);
      key.setService(DEFAULT_SERVICE);
    }
  }

  public static int getMaximumDecoderExceptionsToPrint(JobContext job) {
    return job.getConfiguration().getInt(PRINT_MAX_DECODER_EXCEPTIONS, 10);
  }
}
