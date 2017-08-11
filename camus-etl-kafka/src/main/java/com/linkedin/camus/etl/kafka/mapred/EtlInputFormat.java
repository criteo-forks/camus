package com.linkedin.camus.etl.kafka.mapred;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.coders.MessageDecoder;
import com.linkedin.camus.etl.kafka.CamusJob;
import com.linkedin.camus.etl.kafka.mapred.MapredUtil.EffectiveSeqFileReader;
import io.confluent.camus.etl.kafka.coders.AvroMessageDecoder;
import com.linkedin.camus.etl.kafka.coders.MessageDecoderFactory;
import com.linkedin.camus.etl.kafka.common.EtlKey;
import com.linkedin.camus.etl.kafka.common.EtlRequest;
import com.linkedin.camus.etl.kafka.common.LeaderInfo;
import com.linkedin.camus.workallocater.CamusRequest;
import com.linkedin.camus.workallocater.WorkAllocator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;

import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;


/**
 * Input format for a Kafka pull job.
 */
public class EtlInputFormat extends InputFormat<EtlKey, CamusWrapper> {

  public static final String KAFKA_BLACKLIST_TOPIC = "kafka.blacklist.topics";
  public static final String KAFKA_WHITELIST_TOPIC = "kafka.whitelist.topics";

  public static final String KAFKA_MOVE_TO_LAST_OFFSET_LIST = "kafka.move.to.last.offset.list";
  public static final String KAFKA_MOVE_TO_CLOSEST_OFFSET = "kafka.move.to.closest.offset";

  public static final String KAFKA_MOVE_TO_LAST_OFFSET_ON_FIRST_RUN = "kafka.move.to.last.offset.on.first.run";

  public static final String KAFKA_CLIENT_BUFFER_SIZE = "kafka.client.buffer.size";
  public static final String KAFKA_CLIENT_SO_TIMEOUT = "kafka.client.so.timeout";

  public static final String KAFKA_MAX_PULL_HRS = "kafka.max.pull.hrs";
  public static final String KAFKA_MAX_PULL_MINUTES_PER_TASK = "kafka.max.pull.minutes.per.task";
  public static final String KAFKA_MAX_HISTORICAL_DAYS = "kafka.max.historical.days";

  public static final String CAMUS_MESSAGE_DECODER_CLASS = "camus.message.decoder.class";
  public static final String ETL_IGNORE_SCHEMA_ERRORS = "etl.ignore.schema.errors";
  public static final String ETL_AUDIT_IGNORE_SERVICE_TOPIC_LIST = "etl.audit.ignore.service.topic.list";

  public static final String CAMUS_WORK_ALLOCATOR_CLASS = "camus.work.allocator.class";
  public static final String CAMUS_WORK_ALLOCATOR_DEFAULT = "com.linkedin.camus.workallocater.BaseAllocator";

  private static Logger log = null;

  public EtlInputFormat() {
    if (log == null)
      log = Logger.getLogger(getClass());
  }

  public static void setLogger(Logger log) {
    EtlInputFormat.log = log;
  }

  @Override
  public RecordReader<EtlKey, CamusWrapper> createRecordReader(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    return new EtlRecordReader(this, split, context);
  }

  /**
   * Gets the metadata from Kafka
   *
   * @param context
   * @return
   */
  public List<TopicMetadata> getKafkaMetadata(JobContext context) {
    ArrayList<String> metaRequestTopics = new ArrayList<String>();
    CamusJob.startTiming("kafkaSetupTime");
    String brokerString = CamusJob.getKafkaBrokers(context);
    if (brokerString.isEmpty())
      throw new InvalidParameterException("kafka.brokers must contain at least one node");
    List<String> brokers = Arrays.asList(brokerString.split("\\s*,\\s*"));
    Collections.shuffle(brokers);
    boolean fetchMetaDataSucceeded = false;
    int i = 0;
    List<TopicMetadata> topicMetadataList = null;
    Exception savedException = null;
    while (i < brokers.size() && !fetchMetaDataSucceeded) {
      SimpleConsumer consumer = createBrokerConsumer(context, brokers.get(i));
      log.info(String.format("Fetching metadata from broker %s with client id %s for %d topic(s) %s", brokers.get(i),
          consumer.clientId(), metaRequestTopics.size(), metaRequestTopics));
      try {
        topicMetadataList = consumer.send(new TopicMetadataRequest(metaRequestTopics)).topicsMetadata();
        fetchMetaDataSucceeded = true;
      } catch (Exception e) {
        savedException = e;
        log.warn(
            String.format("Fetching topic metadata with client id %s for topics [%s] from broker [%s] failed",
                consumer.clientId(), metaRequestTopics, brokers.get(i)), e);
      } finally {
        consumer.close();
        i++;
      }
    }
    if (!fetchMetaDataSucceeded) {
      throw new RuntimeException("Failed to obtain metadata!", savedException);
    }
    CamusJob.stopTiming("kafkaSetupTime");
    return topicMetadataList;
  }

  private SimpleConsumer createBrokerConsumer(JobContext context, String broker) {
    if (!broker.matches(".+:\\d+"))
      throw new InvalidParameterException("The kakfa broker " + broker + " must follow address:port pattern");
    String[] hostPort = broker.split(":");
    return createSimpleConsumer(context, hostPort[0], Integer.valueOf(hostPort[1]));
  }

  public SimpleConsumer createSimpleConsumer(JobContext context, String host, int port) {
    SimpleConsumer consumer =
        new SimpleConsumer(host, port,
            CamusJob.getKafkaTimeoutValue(context), CamusJob.getKafkaBufferSize(context),
            CamusJob.getKafkaClientName(context));
    return consumer;
  }

  /**
   * Gets the latest offsets and create the requests as needed
   *
   * @param context
   * @param offsetRequestInfo
   * @return
   */
  public ArrayList<CamusRequest> fetchLatestOffsetAndCreateEtlRequests(JobContext context,
      HashMap<LeaderInfo, ArrayList<TopicAndPartition>> offsetRequestInfo) {
    ArrayList<CamusRequest> finalRequests = new ArrayList<CamusRequest>();
    for (LeaderInfo leader : offsetRequestInfo.keySet()) {
      SimpleConsumer consumer = createSimpleConsumer(context, leader.getUri().getHost(), leader.getUri().getPort());
      // Latest Offset
      PartitionOffsetRequestInfo partitionLatestOffsetRequestInfo =
          new PartitionOffsetRequestInfo(kafka.api.OffsetRequest.LatestTime(), 1);
      // Earliest Offset
      PartitionOffsetRequestInfo partitionEarliestOffsetRequestInfo =
          new PartitionOffsetRequestInfo(kafka.api.OffsetRequest.EarliestTime(), 1);
      Map<TopicAndPartition, PartitionOffsetRequestInfo> latestOffsetInfo =
          new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
      Map<TopicAndPartition, PartitionOffsetRequestInfo> earliestOffsetInfo =
          new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
      ArrayList<TopicAndPartition> topicAndPartitions = offsetRequestInfo.get(leader);
      for (TopicAndPartition topicAndPartition : topicAndPartitions) {
        latestOffsetInfo.put(topicAndPartition, partitionLatestOffsetRequestInfo);
        earliestOffsetInfo.put(topicAndPartition, partitionEarliestOffsetRequestInfo);
      }

      OffsetResponse latestOffsetResponse =
          consumer.getOffsetsBefore(new OffsetRequest(latestOffsetInfo, kafka.api.OffsetRequest.CurrentVersion(),
              CamusJob.getKafkaClientName(context)));
      OffsetResponse earliestOffsetResponse =
          consumer.getOffsetsBefore(new OffsetRequest(earliestOffsetInfo, kafka.api.OffsetRequest.CurrentVersion(),
              CamusJob.getKafkaClientName(context)));
      consumer.close();

      for (TopicAndPartition topicAndPartition : topicAndPartitions) {
          long[] latestOffsets = latestOffsetResponse.offsets(topicAndPartition.topic(), topicAndPartition.partition());
          long[] earliestOffsets =
              earliestOffsetResponse.offsets(topicAndPartition.topic(), topicAndPartition.partition());

          if (latestOffsets.length <= 0 || earliestOffsets.length <= 0) {
              if (latestOffsetResponse.hasError()) {
                  log.error("Latest offsets for topic " + topicAndPartition.topic()
                          + " and partition " + topicAndPartition.partition() + " has errors: "
                          , ErrorMapping.exceptionFor(latestOffsetResponse.errorCode(topicAndPartition.topic(), topicAndPartition.partition())));
              }
              if (earliestOffsetResponse.hasError()) {
                  log.error("Earliest offsets for topic " + topicAndPartition.topic()
                          + " and partition " + topicAndPartition.partition() + " has errors: "
                          , ErrorMapping.exceptionFor(earliestOffsetResponse.errorCode(topicAndPartition.topic(), topicAndPartition.partition())));
              }
          } else {
              //TODO: factor out kafka specific request functionality
              CamusRequest etlRequest =
                  new EtlRequest(context, topicAndPartition.topic(), Integer.toString(leader.getLeaderId()),
                          topicAndPartition.partition(), leader.getUri());
              etlRequest.setLatestOffset(latestOffsets[0]);
              etlRequest.setEarliestOffset(earliestOffsets[0]);
              finalRequests.add(etlRequest);
          }
      }
    }
    return finalRequests;
  }

  public String createTopicRegEx(HashSet<String> topicsSet) {
    String regex = "";
    StringBuilder stringbuilder = new StringBuilder();
    for (String whiteList : topicsSet) {
      stringbuilder.append(whiteList);
      stringbuilder.append("|");
    }
    regex = "(" + stringbuilder.substring(0, stringbuilder.length() - 1) + ")";
    Pattern.compile(regex);
    return regex;
  }

  public List<TopicMetadata> filterWhitelistTopics(List<TopicMetadata> topicMetadataList,
      HashSet<String> whiteListTopics) {
    ArrayList<TopicMetadata> filteredTopics = new ArrayList<TopicMetadata>();
    String regex = createTopicRegEx(whiteListTopics);
    for (TopicMetadata topicMetadata : topicMetadataList) {
      if (Pattern.matches(regex, topicMetadata.topic())) {
        filteredTopics.add(topicMetadata);
      } else {
        log.info("Discarding topic : " + topicMetadata.topic());
      }
    }
    return filteredTopics;
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
    CamusJob.startTiming("getSplits");
    ArrayList<CamusRequest> finalRequests;
    HashMap<LeaderInfo, ArrayList<TopicAndPartition>> offsetRequestInfo =
        new HashMap<LeaderInfo, ArrayList<TopicAndPartition>>();
    try {

      // Get Metadata for all topics
      List<TopicMetadata> topicMetadataList = getKafkaMetadata(context);

      // Filter any white list topics
      HashSet<String> whiteListTopics = new HashSet<String>(Arrays.asList(getKafkaWhitelistTopic(context)));
      if (!whiteListTopics.isEmpty()) {
        topicMetadataList = filterWhitelistTopics(topicMetadataList, whiteListTopics);
      }

      // Filter all blacklist topics
      HashSet<String> blackListTopics = new HashSet<String>(Arrays.asList(getKafkaBlacklistTopic(context)));
      String regex = "";
      if (!blackListTopics.isEmpty()) {
        regex = createTopicRegEx(blackListTopics);
      }

      for (TopicMetadata topicMetadata : topicMetadataList) {
        if (Pattern.matches(regex, topicMetadata.topic())) {
          log.info("Discarding topic (blacklisted): " + topicMetadata.topic());
        } else if (!createMessageDecoder(context, topicMetadata.topic())) {
          log.info("Discarding topic (Decoder generation failed) : " + topicMetadata.topic());
        } else if (topicMetadata.errorCode() != ErrorMapping.NoError()) {
          log.info("Skipping the creation of ETL request for Whole Topic : " + topicMetadata.topic() + " Exception : "
              + ErrorMapping.exceptionFor(topicMetadata.errorCode()));
        } else {
          for (PartitionMetadata partitionMetadata : topicMetadata.partitionsMetadata()) {
            // We only care about LeaderNotAvailableCode error on partitionMetadata level
            // Error codes such as ReplicaNotAvailableCode should not stop us.
            if (partitionMetadata.errorCode() == ErrorMapping.LeaderNotAvailableCode()) {
              log.info("Skipping the creation of ETL request for Topic : " + topicMetadata.topic()
                  + " and Partition : " + partitionMetadata.partitionId() + " Exception : "
                  + ErrorMapping.exceptionFor(partitionMetadata.errorCode()));
            } else {
              if (partitionMetadata.errorCode() != ErrorMapping.NoError()) {
                log.warn("Receiving non-fatal error code, Continuing the creation of ETL request for Topic : "
                    + topicMetadata.topic() + " and Partition : " + partitionMetadata.partitionId() + " Exception : "
                    + ErrorMapping.exceptionFor(partitionMetadata.errorCode()));
              }
              LeaderInfo leader =
                  new LeaderInfo(new URI("tcp://" + partitionMetadata.leader().connectionString()),
                      partitionMetadata.leader().id());
              if (offsetRequestInfo.containsKey(leader)) {
                ArrayList<TopicAndPartition> topicAndPartitions = offsetRequestInfo.get(leader);
                topicAndPartitions.add(new TopicAndPartition(topicMetadata.topic(), partitionMetadata.partitionId()));
                offsetRequestInfo.put(leader, topicAndPartitions);
              } else {
                ArrayList<TopicAndPartition> topicAndPartitions = new ArrayList<TopicAndPartition>();
                topicAndPartitions.add(new TopicAndPartition(topicMetadata.topic(), partitionMetadata.partitionId()));
                offsetRequestInfo.put(leader, topicAndPartitions);
              }

            }
          }
        }
      }
    } catch (Exception e) {
      log.error("Unable to pull requests from Kafka brokers. Exiting the program", e);
      throw new IOException("Unable to pull requests from Kafka brokers.", e);
    }
    // Get the latest offsets and generate the EtlRequests
    finalRequests = fetchLatestOffsetAndCreateEtlRequests(context, offsetRequestInfo);

    Collections.sort(finalRequests, new Comparator<CamusRequest>() {
      public int compare(CamusRequest r1, CamusRequest r2) {
        return r1.getTopic().compareTo(r2.getTopic());
      }
    });

    final Configuration conf = context.getConfiguration();
    if(conf.getBoolean("mapred.map.tasks.dynamic", true)) {
      final int numMapFit = Math.max(1, finalRequests.size() / conf.getInt("camus.map.tasks.topic",2));
      final int numMapMax = conf.getInt("mapred.map.tasks.max", 1000);
      conf.set("mapred.map.tasks",
              Integer.toString(Math.min(numMapFit, numMapMax)));
    }

    if (log.isInfoEnabled()) {
      StringBuilder sb = new StringBuilder();
      for (CamusRequest req : finalRequests) {
        sb.append(req).append("\n");
      }
      log.info("The requests from kafka metadata are: \n" + sb.toString());
    }
    writeRequests(finalRequests, context);
    Map<CamusRequest, EtlKey> offsetKeys = getPreviousOffsets(FileInputFormat.getInputPaths(context), context);
    HashSet<String> alreadySeen = getAlreadySeenTopics(offsetKeys);

    Set<String> moveLatest = getMoveToLatestTopicsSet(context);
    boolean shouldMoveLatestIfNew = getKafkaMoveToLatestOffsetOnFirstRun(context);
    for (CamusRequest request : finalRequests) {
      String topic = request.getTopic();
      if (moveLatest.contains(topic) || moveLatest.contains("all")
              || ( shouldMoveLatestIfNew && ! alreadySeen.contains(topic))) {
        log.info("Moving to latest for topic: " + request.getTopic());
        //TODO: factor out kafka specific request functionality
        EtlKey oldKey = offsetKeys.get(request);
        EtlKey newKey =
            new EtlKey(request.getTopic(), ((EtlRequest) request).getLeaderId(), request.getPartition(), 0,
                request.getLastOffset());

        if (oldKey != null)
          newKey.setMessageSize(oldKey.getMessageSize());

        offsetKeys.put(request, newKey);
      }

      EtlKey key = offsetKeys.get(request);

      if (key != null) {
        request.setOffset(key.getOffset());
        request.setAvgMsgSize(key.getMessageSize());
      }

      if (request.getEarliestOffset() > request.getOffset() || request.getOffset() > request.getLastOffset()) {
        if (request.getEarliestOffset() > request.getOffset()) {
          log.error("The earliest offset was found to be more than the current offset: " + request);
        } else {
          log.error("The current offset was found to be more than the latest offset: " + request);
        }

        boolean moveToClosestOffset = context.getConfiguration().getBoolean(KAFKA_MOVE_TO_CLOSEST_OFFSET, false);
        boolean offsetUnset = request.getOffset() == EtlRequest.DEFAULT_OFFSET;
        log.info("move_to_closest: " + moveToClosestOffset + " offset_unset: " + offsetUnset);

        // If the offset is before the earliest offset of the partition and the setting move_to_closest_offset is on,
        // we begin from the earliest offset.
        // When the offset is unset, it means it's a new topic/partition, we also need to consume the earliest offset
        if ((request.getOffset() < request.getEarliestOffset() && moveToClosestOffset) || offsetUnset) {
          log.error("Moving to the earliest offset available");
          request.setOffset(request.getEarliestOffset());
        } else if (request.getLastOffset() < request.getOffset() && moveToClosestOffset) {
          // If the offset is behind the latest offset and the setting move_to_closest_offset is on,
          // move the offset to the latest available offset.
          log.error("The previous offset is behind the latest offset of the partition, " +
                    "moving to the latest offset according to the setting.");
          request.setOffset(request.getLastOffset());
        } else {
          log.error(new StringBuilder()
                    .append("Offset range from kafka metadata is outside the previously persisted offset,")
                    .append(" please check whether kafka cluster configuration is correct.")
                    .append(" You can also specify config parameter: ").append(KAFKA_MOVE_TO_CLOSEST_OFFSET)
                    .append(" to start processing from earliest kafka metadata offset.")
                    .append(" if the previous offset is before that position")
                    .append(" or to start processing from the last kafka metadata offset")
                    .append(" if behind that position")
                    .toString());
          throw new IOException("Offset from kafka metadata is out of range: " + request);
        }
        offsetKeys.put(
            request,
            //TODO: factor out kafka specific request functionality
            new EtlKey(request.getTopic(), ((EtlRequest) request).getLeaderId(), request.getPartition(), 0, request
                       .getOffset()));
      }
      log.info(request);
    }

    writePrevious(offsetKeys.values(), context);

    CamusJob.stopTiming("getSplits");
    CamusJob.startTiming("hadoop");
    CamusJob.setTime("hadoop_start");

    WorkAllocator allocator = getWorkAllocator(context);
    Properties props = new Properties();
    props.putAll(context.getConfiguration().getValByRegex(".*"));
    allocator.init(props);

    return allocator.allocateWork(finalRequests, context);
  }

  private HashSet<String> getAlreadySeenTopics(Map<CamusRequest, EtlKey> offsetKeys) {
    HashSet<String> topics = new HashSet<String>();
    for(EtlKey key:offsetKeys.values()){
      topics.add(key.getTopic());
    }
    return topics;
  }

  private Set<String> stringsToSet(String[] items){
    Set<String> result = new HashSet<String>();
    if (items == null)
      return result;
    for (String item:items)
      result.add(item);
    return result;
  }

  private Set<String> getMoveToLatestTopicsSet(JobContext context) {
     return stringsToSet(getMoveToLatestTopics(context));
  }


  private boolean createMessageDecoder(JobContext context, String topic) {
    try {
      MessageDecoderFactory.createMessageDecoder(context, topic);
      return true;
    } catch (Exception e) {
      log.error("failed to create decoder", e);
      return false;
    }
  }

  private void writePrevious(Collection<EtlKey> missedKeys, JobContext context) throws IOException {
    FileSystem fs = FileSystem.get(context.getConfiguration());
    Path output = FileOutputFormat.getOutputPath(context);

    if (fs.exists(output)) {
      fs.mkdirs(output);
    }

    output = new Path(output, EtlMultiOutputFormat.OFFSET_PREFIX + "-previous");
    SequenceFile.Writer writer =
        SequenceFile.createWriter(context.getConfiguration(),
            SequenceFile.Writer.file(output),
            SequenceFile.Writer.keyClass(EtlKey.class),
            SequenceFile.Writer.valueClass(NullWritable.class),
            EtlConfigurationUtils.smallBlockSizeOption(context.getConfiguration()));

    for (EtlKey key : missedKeys) {
      writer.append(key, NullWritable.get());
    }

    writer.close();
  }

  private void writeRequests(List<CamusRequest> requests, JobContext context) throws IOException {
    FileSystem fs = FileSystem.get(context.getConfiguration());
    Path output = FileOutputFormat.getOutputPath(context);

    if (fs.exists(output)) {
      fs.mkdirs(output);
    }

    output = new Path(output, EtlMultiOutputFormat.REQUESTS_FILE);
    SequenceFile.Writer writer =
        SequenceFile.createWriter(context.getConfiguration(),
            SequenceFile.Writer.file(output),
            SequenceFile.Writer.keyClass(EtlRequest.class),
            SequenceFile.Writer.valueClass(NullWritable.class),
            EtlConfigurationUtils.smallBlockSizeOption(context.getConfiguration()));

    for (CamusRequest r : requests) {
      //TODO: factor out kafka specific request functionality
      writer.append((EtlRequest) r, NullWritable.get());
    }
    writer.close();
  }

  /**
   * Fetch previous offsets in parallel with an executor service of at most 10 executors
   */
  public Map<CamusRequest, EtlKey> getPreviousOffsets(Path[] inputs, final JobContext context) throws IOException {
    Map<CamusRequest, EtlKey> offsetKeysMap = new HashMap<>();
    for (Path input : inputs) {
      EffectiveSeqFileReader<EtlKey, NullWritable> reader = new EffectiveSeqFileReader<EtlKey, NullWritable>(
              context.getConfiguration(), input, new OffsetFileFilter()
      ) {
        @Override
        public EtlKey createKey() {
          return new EtlKey();
        }
        @Override
        public NullWritable createValue() {
          return NullWritable.get();
        }
        @Override
        public EtlKey copyKey(EtlKey key) {
          return new EtlKey(key);
        }
        @Override
        public NullWritable copyValue(NullWritable value) {
          return NullWritable.get();
        }
      };
      Set<EtlKey> previousOffsets = reader.read().keySet();

      for (EtlKey key : previousOffsets) {
        CamusRequest request = new EtlRequest(context, key.getTopic(), key.getLeaderId(), key.getPartition());
        if (offsetKeysMap.containsKey(request)) {

          EtlKey oldKey = offsetKeysMap.get(request);
          if (oldKey.getOffset() < key.getOffset()) {
            offsetKeysMap.put(request, key);
          }
        } else {
          offsetKeysMap.put(request, key);
        }
      }
    }
    return offsetKeysMap;
  }

  public static void setWorkAllocator(JobContext job, Class<WorkAllocator> val) {
    job.getConfiguration().setClass(CAMUS_WORK_ALLOCATOR_CLASS, val, WorkAllocator.class);
  }

  public static WorkAllocator getWorkAllocator(JobContext job) {
    try {
      return (WorkAllocator) job.getConfiguration()
          .getClass(CAMUS_WORK_ALLOCATOR_CLASS, Class.forName(CAMUS_WORK_ALLOCATOR_DEFAULT)).newInstance();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static void setMoveToLatestTopics(JobContext job, String val) {
    job.getConfiguration().set(KAFKA_MOVE_TO_LAST_OFFSET_LIST, val);
  }

  public static String[] getMoveToLatestTopics(JobContext job) {
    return job.getConfiguration().getStrings(KAFKA_MOVE_TO_LAST_OFFSET_LIST);
  }

  public static void setKafkaClientBufferSize(JobContext job, int val) {
    job.getConfiguration().setInt(KAFKA_CLIENT_BUFFER_SIZE, val);
  }

  public static int getKafkaClientBufferSize(JobContext job) {
    return job.getConfiguration().getInt(KAFKA_CLIENT_BUFFER_SIZE, 2 * 1024 * 1024);
  }

  public static void setKafkaClientTimeout(JobContext job, int val) {
    job.getConfiguration().setInt(KAFKA_CLIENT_SO_TIMEOUT, val);
  }

  public static int getKafkaClientTimeout(JobContext job) {
    return job.getConfiguration().getInt(KAFKA_CLIENT_SO_TIMEOUT, 60000);
  }

  public static void setKafkaMaxPullHrs(JobContext job, int val) {
    job.getConfiguration().setInt(KAFKA_MAX_PULL_HRS, val);
  }

  public static int getKafkaMaxPullHrs(JobContext job) {
    return job.getConfiguration().getInt(KAFKA_MAX_PULL_HRS, -1);
  }

  public static void setKafkaMaxPullMinutesPerTask(JobContext job, int val) {
    job.getConfiguration().setInt(KAFKA_MAX_PULL_MINUTES_PER_TASK, val);
  }

  public static int getKafkaMaxPullMinutesPerTask(JobContext job) {
    return job.getConfiguration().getInt(KAFKA_MAX_PULL_MINUTES_PER_TASK, -1);
  }

  public static void setKafkaMaxHistoricalDays(JobContext job, int val) {
    job.getConfiguration().setInt(KAFKA_MAX_HISTORICAL_DAYS, val);
  }

  public static int getKafkaMaxHistoricalDays(JobContext job) {
    return job.getConfiguration().getInt(KAFKA_MAX_HISTORICAL_DAYS, -1);
  }

  public static void setKafkaBlacklistTopic(JobContext job, String val) {
    job.getConfiguration().set(KAFKA_BLACKLIST_TOPIC, val);
  }

  public static boolean getKafkaMoveToLatestOffsetOnFirstRun(JobContext job){
    return job.getConfiguration().getBoolean(KAFKA_MOVE_TO_LAST_OFFSET_ON_FIRST_RUN, false);
  }

  public static void  setKafkaMoveToLatestOffsetOnFirstRun(JobContext job, boolean val){
    job.getConfiguration().setBoolean(KAFKA_MOVE_TO_LAST_OFFSET_ON_FIRST_RUN, val);
  }

  public static String[] getKafkaBlacklistTopic(JobContext job) {
    if (job.getConfiguration().get(KAFKA_BLACKLIST_TOPIC) != null
        && !job.getConfiguration().get(KAFKA_BLACKLIST_TOPIC).isEmpty()) {
      return job.getConfiguration().getStrings(KAFKA_BLACKLIST_TOPIC);
    } else {
      return new String[] {};
    }
  }

  public static void setKafkaWhitelistTopic(JobContext job, String val) {
    job.getConfiguration().set(KAFKA_WHITELIST_TOPIC, val);
  }

  public static String[] getKafkaWhitelistTopic(JobContext job) {
    if (job.getConfiguration().get(KAFKA_WHITELIST_TOPIC) != null
        && !job.getConfiguration().get(KAFKA_WHITELIST_TOPIC).isEmpty()) {
      return job.getConfiguration().getStrings(KAFKA_WHITELIST_TOPIC);
    } else {
      return new String[] {};
    }
  }

  public static void setEtlIgnoreSchemaErrors(JobContext job, boolean val) {
    job.getConfiguration().setBoolean(ETL_IGNORE_SCHEMA_ERRORS, val);
  }

  public static boolean getEtlIgnoreSchemaErrors(JobContext job) {
    return job.getConfiguration().getBoolean(ETL_IGNORE_SCHEMA_ERRORS, false);
  }

  public static void setEtlAuditIgnoreServiceTopicList(JobContext job, String topics) {
    job.getConfiguration().set(ETL_AUDIT_IGNORE_SERVICE_TOPIC_LIST, topics);
  }

  public static String[] getEtlAuditIgnoreServiceTopicList(JobContext job) {
    return job.getConfiguration().getStrings(ETL_AUDIT_IGNORE_SERVICE_TOPIC_LIST, "");
  }

  public static void setMessageDecoderClass(JobContext job, Class<MessageDecoder> cls) {
    job.getConfiguration().setClass(CAMUS_MESSAGE_DECODER_CLASS, cls, MessageDecoder.class);
  }

  public static Class<MessageDecoder> getMessageDecoderClass(JobContext job) {
    return (Class<MessageDecoder>) job.getConfiguration().getClass(CAMUS_MESSAGE_DECODER_CLASS,
        AvroMessageDecoder.class);
  }

  public static Class<MessageDecoder> getMessageDecoderClass(JobContext job, String topicName) {
    Class<MessageDecoder> topicDecoder = (Class<MessageDecoder>) job.getConfiguration().getClass(
            CAMUS_MESSAGE_DECODER_CLASS + "." + topicName, null);
    return topicDecoder == null ? getMessageDecoderClass(job) : topicDecoder;
  }

  private class OffsetFileFilter implements PathFilter {

    @Override
    public boolean accept(Path arg0) {
      return arg0.getName().startsWith(EtlMultiOutputFormat.OFFSET_PREFIX);
    }
  }
}
