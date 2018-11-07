package com.linkedin.camus.etl.kafka;

import com.google.gson.Gson;
import com.linkedin.camus.etl.kafka.coders.JsonStringMessageDecoder;
import com.linkedin.camus.etl.kafka.common.SequenceFileRecordWriterProvider;
import com.linkedin.camus.etl.kafka.mapred.EtlInputFormat;
import com.linkedin.camus.etl.kafka.mapred.EtlMultiOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.easymock.EasyMock.*;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class CamusJobTestWithMock {

  private static final Random RANDOM = new Random();

  private static final String BASE_PATH = "/camus";
  private static final String DESTINATION_PATH = BASE_PATH + "/destination";
  private static final String EXECUTION_BASE_PATH = BASE_PATH + "/execution";
  private static final String EXECUTION_HISTORY_PATH = EXECUTION_BASE_PATH + "/history";

  private static final String KAFKA_HOST = "localhost";
  private static final int KAFKA_PORT = 2121;
  private static final int KAFKA_TIMEOUT_VALUE = 1000;
  private static final int KAFKA_BUFFER_SIZE = 1024;
  private static final String KAFKA_CLIENT_ID = "Camus";

  private static final String TOPIC_1 = "topic_1";
  private static final int PARTITION_1_ID = 0;
  private static final long EARLY_OFFSET = 0;
  private static final long LATE_OFFSET = 1;

  private static FileSystem fs;
  private static Gson gson;
  private static Map<String, List<MyMessage>> messagesWritten;

  // mock objects
  private static List<Object> _mocks = new ArrayList<>();
  private static KafkaConsumer<byte[], byte[]> consumer;

  @BeforeClass
  public static void beforeClass() throws IOException {
    fs = FileSystem.get(new Configuration());
    gson = new Gson();

    // You can't delete messages in Kafka so just writing a set of known messages that can be used for testing
    messagesWritten = new HashMap<>();
    messagesWritten.put(TOPIC_1, writeKafka(TOPIC_1, 1));
  }

  @AfterClass
  public static void afterClass() {
  }

  private Properties props;
  private CamusJob job;
  private TemporaryFolder folder;
  private String destinationPath;

  @Before
  public void before() throws IOException, NoSuchFieldException, IllegalAccessException {
    resetCamus();

    folder = new TemporaryFolder();
    folder.create();

    String path = folder.getRoot().getAbsolutePath();
    destinationPath = path + DESTINATION_PATH;

    props = new Properties();

    props.setProperty(EtlMultiOutputFormat.ETL_DESTINATION_PATH, destinationPath);
    props.setProperty(CamusJob.ETL_EXECUTION_BASE_PATH, path + EXECUTION_BASE_PATH);
    props.setProperty(CamusJob.ETL_EXECUTION_HISTORY_PATH, path + EXECUTION_HISTORY_PATH);

    props.setProperty(EtlInputFormat.CAMUS_MESSAGE_DECODER_CLASS, JsonStringMessageDecoder.class.getName());
    props.setProperty(EtlMultiOutputFormat.ETL_RECORD_WRITER_PROVIDER_CLASS,
        SequenceFileRecordWriterProvider.class.getName());

    props.setProperty(EtlMultiOutputFormat.ETL_RUN_TRACKING_POST, Boolean.toString(false));
    props.setProperty("kafka.consumer." + ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_HOST + ":" + KAFKA_PORT);
    props.setProperty(CamusJob.KAFKA_CONSUMER_POLL_TIMEOUT_MS, Integer.toString(KAFKA_TIMEOUT_VALUE));


    // Run Map/Reduce tests in process for hadoop2  
    props.setProperty("mapreduce.framework.name", "local");
    // Run M/R for Hadoop1
    props.setProperty("mapreduce.jobtracker.address", "local");

    job = new CamusJob(props);
  }

  @After
  public void after() {
    // Delete all camus data
    folder.delete();
  }

  @Test
  public void runJob1() throws Exception {
    setupJob1();

    // Run a second time (no additional messages should be found)
    job = new CamusJob(props);
    job.run(TestEtlInputFormat.class, EtlMultiOutputFormat.class);

    verify(_mocks.toArray());
    verifyJob1();
  }

  @SuppressWarnings("unchecked")
  private void setupJob1() {
    // For topicMetadata
    Map<String, List<PartitionInfo>> topicMetadata = new HashMap<>();
    Node node = new Node(1, "localhost", 54321);
    Node[] replicas = new Node[] { node };
    Node[] isrs = new Node[] { node };
    PartitionInfo info = new PartitionInfo(TOPIC_1, PARTITION_1_ID, node, replicas, isrs);
    topicMetadata.put(TOPIC_1, Collections.singletonList(info));

    // For OffsetResponse
    TopicPartition topicPartition = new TopicPartition(TOPIC_1, PARTITION_1_ID);
    Map<TopicPartition, Long> END_OFFSETS = new HashMap<>(1);
    END_OFFSETS.put(topicPartition, LATE_OFFSET);

    Map<TopicPartition, Long> BEGINNING_OFFSETS = new HashMap<>(1);
    BEGINNING_OFFSETS.put(topicPartition, EARLY_OFFSET);

    // For SimpleConsumer.fetch()
    List<MyMessage> myMessages = messagesWritten.get(TOPIC_1);
    MyMessage myMessage = myMessages.get(0);
    String payload = gson.toJson(myMessage);
    String msgKey = Integer.toString(myMessage.number);
    ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>(TOPIC_1, PARTITION_1_ID, EARLY_OFFSET, msgKey.getBytes(), payload.getBytes());
    Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> records = new HashMap<>(1);
    records.put(topicPartition, Collections.singletonList(record));
    ConsumerRecords<byte[], byte[]> consumerRecords = new ConsumerRecords<>(records);

    // For SimpleConsumer
    consumer = createMock(KafkaConsumer.class);
    _mocks.add(consumer);
    expect(consumer.listTopics()).andReturn(topicMetadata).times(1);

    consumer.assign(anyObject());
    expectLastCall().times(1);

    expect(consumer.endOffsets(anyObject())).andReturn(END_OFFSETS).times(1);
    expect(consumer.beginningOffsets(anyObject())).andReturn(BEGINNING_OFFSETS).times(1);
    expect(consumer.poll(anyLong())).andReturn(consumerRecords).times(1);

    consumer.close();
    expectLastCall().times(3);

    consumer.seek(topicPartition, EARLY_OFFSET);
    expectLastCall().times(1);

    replay(_mocks.toArray());
  }

  private void verifyJob1() throws Exception {
    assertCamusContains(TOPIC_1);
  }

  private void assertCamusContains(String topic) throws InstantiationException, IllegalAccessException, IOException {
    assertCamusContains(topic, messagesWritten.get(topic));
  }

  private void assertCamusContains(String topic, List<MyMessage> messages) throws InstantiationException,
          IllegalAccessException, IOException {
    List<MyMessage> readMessages = readMessages(topic);
    assertThat(readMessages.size(), is(messages.size()));
    assertTrue(readMessages(topic).containsAll(messages));
  }

  private static List<MyMessage> writeKafka(String topic, int numOfMessages) {
    return IntStream.of(numOfMessages)
            .mapToObj(i -> new MyMessage(RANDOM.nextInt()))
            .collect(Collectors.toList());
  }

  private List<MyMessage> readMessages(String topic) throws IOException, InstantiationException, IllegalAccessException {
    return readMessages(new Path(destinationPath, topic));
  }

  private List<MyMessage> readMessages(Path path) throws IOException, InstantiationException, IllegalAccessException {
    List<MyMessage> messages = new ArrayList<MyMessage>();

    try {
      for (FileStatus file : fs.listStatus(path)) {
        if (file.isDir()) {
          messages.addAll(readMessages(file.getPath()));
        } else {
          SequenceFile.Reader reader = new SequenceFile.Reader(fs, file.getPath(), new Configuration());
          try {
            LongWritable key = (LongWritable) reader.getKeyClass().newInstance();
            Text value = (Text) reader.getValueClass().newInstance();

            while (reader.next(key, value)) {
              messages.add(gson.fromJson(value.toString(), MyMessage.class));
            }
          } finally {
            reader.close();
          }
        }
      }
    } catch (FileNotFoundException e) {
      System.out.println("No camus messages were found in [" + path + "]");
    }

    return messages;
  }

  private static class TestEtlInputFormat extends EtlInputFormat {
    public TestEtlInputFormat() {
      super();
    }

    public static void setLogger(Logger log) {
      EtlInputFormat.setLogger(log);
    }

    @Override
    public KafkaConsumer<byte[], byte[]> createKafkaConsumer(JobContext context) {
      return consumer;
    }
  }

  private static class MyMessage {

    private int number;

    // Used by Gson
    public MyMessage() {
    }

    public MyMessage(int number) {
      this.number = number;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null || !(obj instanceof MyMessage))
        return false;

      MyMessage other = (MyMessage) obj;

      return number == other.number;
    }
  }

  private static void resetCamus() throws NoSuchFieldException, IllegalAccessException {
    _mocks.clear();
    consumer = null;

    // The EtlMultiOutputFormat has a static private field called committer which is only created if null. The problem is this
    // writes the Camus metadata meaning the first execution of the camus job defines where all committed output goes causing us
    // problems if you want to run Camus again using the meta data (i.e. what offsets we processed). Setting it null here forces
    // it to re-instantiate the object with the appropriate output path

    Field field = EtlMultiOutputFormat.class.getDeclaredField("committer");
    field.setAccessible(true);
    field.set(null, null);
  }

}
