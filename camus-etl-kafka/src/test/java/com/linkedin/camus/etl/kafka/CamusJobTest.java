package com.linkedin.camus.etl.kafka;

import com.google.gson.Gson;
import com.linkedin.camus.etl.kafka.coders.FailDecoder;
import com.linkedin.camus.etl.kafka.coders.JsonStringMessageDecoder;
import com.linkedin.camus.etl.kafka.common.EtlKey;
import com.linkedin.camus.etl.kafka.common.SequenceFileRecordWriterProvider;
import com.linkedin.camus.etl.kafka.mapred.EtlInputFormat;
import com.linkedin.camus.etl.kafka.mapred.EtlMultiOutputFormat;
import com.linkedin.camus.workallocater.CamusRequest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;


public class CamusJobTest {

  private static final Random RANDOM = new Random();

  private static final String BASE_PATH = "/camus";
  private static final String DESTINATION_PATH = BASE_PATH + "/destination";
  private static final String EXECUTION_BASE_PATH = BASE_PATH + "/execution";
  private static final String EXECUTION_HISTORY_PATH = EXECUTION_BASE_PATH + "/history";

  private static final String TOPIC_1 = "topic_1";
  private static final String TOPIC_2 = "topic_2";
  private static final String TOPIC_3 = "topic_3";

  private static KafkaCluster cluster;
  private static String brokersUrl;
  private static FileSystem fs;
  private static Gson gson;
  private static Map<String, List<Message>> messagesWritten;
  private static final long NUMBER_OF_MESSAGE_TO_INSERT = 10l;

  @BeforeClass
  public static void beforeClass() throws IOException {
    cluster = new KafkaCluster();
    brokersUrl = cluster.getBrokers().stream()
            .map(broker -> broker.adminManager().config().hostName() + ":" + broker.adminManager().config().port())
            .collect(Collectors.joining(","));

    fs = FileSystem.get(new Configuration());
    gson = new Gson();

    // You can't delete messages in Kafka so just writing a set of known messages that can be used for testing
    messagesWritten = new HashMap<>();
    messagesWritten.put(TOPIC_1, writeKafka(TOPIC_1, NUMBER_OF_MESSAGE_TO_INSERT));
    messagesWritten.put(TOPIC_2, writeKafka(TOPIC_2, NUMBER_OF_MESSAGE_TO_INSERT));
    messagesWritten.put(TOPIC_3, writeKafka(TOPIC_3, NUMBER_OF_MESSAGE_TO_INSERT));
  }

  @AfterClass
  public static void afterClass() {
    cluster.shutdown();
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

    props = cluster.getProps();

    props.setProperty(EtlMultiOutputFormat.ETL_DESTINATION_PATH, destinationPath);
    props.setProperty(CamusJob.ETL_EXECUTION_BASE_PATH, path + EXECUTION_BASE_PATH);
    props.setProperty(CamusJob.ETL_EXECUTION_HISTORY_PATH, path + EXECUTION_HISTORY_PATH);

    props.setProperty(EtlInputFormat.CAMUS_MESSAGE_DECODER_CLASS, JsonStringMessageDecoder.class.getName());
    props.setProperty(EtlMultiOutputFormat.ETL_RECORD_WRITER_PROVIDER_CLASS,
        SequenceFileRecordWriterProvider.class.getName());

    props.setProperty(EtlMultiOutputFormat.ETL_RUN_TRACKING_POST, Boolean.toString(false));

    // Run Map/Reduce tests in process.
    //props.setProperty("mapreduce.framework.name", "local");
    props.setProperty("mapreduce.jobtracker.address", "local");

    job = new CamusJob(props);
  }

  @After
  public void after() {
    Properties props = new Properties();
    props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokersUrl);
    props.setProperty(ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG, "false");
    KafkaAdminClient adminClient = (KafkaAdminClient) AdminClient.create(props);
    adminClient.deleteTopics(Collections.singletonList("__consumer_offsets"));

    // Delete all camus data
    folder.delete();
  }

  private Map<CamusRequest, EtlKey> fetchLatestOffset() throws Exception {
    return fetchLatestOffset(job, props);
  }

  public static Map<CamusRequest, EtlKey> fetchLatestOffset(CamusJob job, Properties props) throws Exception {
    // Read back latest camus history offsets
    EtlInputFormat inputFormat = new EtlInputFormat();
    Job hJob = job.createJob(props);
    FileSystem fs = FileSystem.get(hJob.getConfiguration());
    Path execHistory = new Path(job.getConf().get(CamusJob.ETL_EXECUTION_HISTORY_PATH));
    FileStatus[] executions = fs.listStatus(execHistory);
    Arrays.sort(executions, Comparator.comparing(f -> f.getPath().getName()));
    // Read only latest execution history folder
    assertThat(executions.length > 0, is(true));
    Path previous = executions[executions.length - 1].getPath();
    FileInputFormat.setInputPaths(hJob, previous);
    JobContext context = new JobContextImpl(hJob.getConfiguration(), new JobID());
    return inputFormat.getPreviousOffsets(FileInputFormat.getInputPaths(context), context);
  }

  @Test
  public void runJob() throws Exception {
    job.run();

    assertCamusContains(TOPIC_1);
    assertCamusContains(TOPIC_2);
    assertCamusContains(TOPIC_3);

    Map<CamusRequest, EtlKey> previousOffsets = fetchLatestOffset();
    assertThat(previousOffsets.values().size(), is(3));

    // Assert that offset are corrects
    for(EtlKey key : previousOffsets.values()) {
      assertThat(key.getOffset(), is(NUMBER_OF_MESSAGE_TO_INSERT));
    }

    // Run a second time (no additional messages should be found)
    job = new CamusJob(props);
    job.run();

    assertCamusContains(TOPIC_1);
    assertCamusContains(TOPIC_2);
    assertCamusContains(TOPIC_3);
  }

  @Test
  public void runJobWithErrors() throws Exception {
    props.setProperty(EtlInputFormat.CAMUS_MESSAGE_DECODER_CLASS, FailDecoder.class.getName());
    job = new CamusJob(props);
    job.run();

    assertThat(readMessages(TOPIC_1).isEmpty(), is(true));
    assertThat(readMessages(TOPIC_2).isEmpty(), is(true));
    assertThat(readMessages(TOPIC_3).isEmpty(), is(true));
  }

  @Test
  public void runJobWithGoToLastOffset() throws Exception {
    props.setProperty(EtlInputFormat.KAFKA_MOVE_TO_LAST_OFFSET_ON_FIRST_RUN, "true");
    job = new CamusJob(props);
    job.run();

    assertThat(readMessages(TOPIC_1).isEmpty(), is(true));
    assertThat(readMessages(TOPIC_2).isEmpty(), is(true));
    assertThat(readMessages(TOPIC_3).isEmpty(), is(true));

    props.setProperty(EtlInputFormat.KAFKA_MOVE_TO_LAST_OFFSET_ON_FIRST_RUN, "false");
    job.run();
    //Since we're already at the end of the job, nothing should be read either

    assertThat(readMessages(TOPIC_1).isEmpty(), is(true));
    assertThat(readMessages(TOPIC_2).isEmpty(), is(true));
    assertThat(readMessages(TOPIC_3).isEmpty(), is(true));

  }

  @Test
  public void runJobWithoutErrorsAndFailOnErrors() throws Exception {
    props.setProperty(CamusJob.ETL_FAIL_ON_ERRORS, Boolean.TRUE.toString());
    job = new CamusJob(props);
    runJob();
  }

  @Test(expected = RuntimeException.class)
  public void runJobWithErrorsAndFailOnErrors() throws Exception {
    props.setProperty(CamusJob.ETL_FAIL_ON_ERRORS, Boolean.TRUE.toString());
    props.setProperty(EtlInputFormat.CAMUS_MESSAGE_DECODER_CLASS, FailDecoder.class.getName());
    job = new CamusJob(props);
    job.run();
  }

  private void assertCamusContains(String topic) throws InstantiationException, IllegalAccessException, IOException {
    assertCamusContains(topic, messagesWritten.get(topic));
  }

  private void assertCamusContains(String topic, List<Message> messages) throws InstantiationException,
      IllegalAccessException, IOException {
    List<Message> readMessages = readMessages(topic);
    assertThat(readMessages.size(), is(messages.size()));
    assertTrue(readMessages(topic).containsAll(messages));
  }

  private static List<Message> writeKafka(String topic, long numOfMessages) {
    List<Message> messages = new ArrayList<>();

    Properties props = cluster.getProps();
    props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokersUrl);
    props.setProperty("value.serializer", ByteArraySerializer.class.getName());
    props.setProperty("key.serializer", ByteArraySerializer.class.getName());


    try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props)) {
      for (long i = 0; i < numOfMessages; i++) {
        Message msg = new Message(RANDOM.nextInt());
        ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>(topic, null, gson.toJson(msg).getBytes(StandardCharsets.UTF_8));
        messages.add(msg);
        producer.send(producerRecord);
      }
    }

    return messages;
  }

  private List<Message> readMessages(String topic) throws IOException, InstantiationException, IllegalAccessException {
    return readMessages(new Path(destinationPath, topic));
  }

  private List<Message> readMessages(Path path) throws IOException, InstantiationException, IllegalAccessException {
    List<Message> messages = new ArrayList<>();

    try {
      for (FileStatus file : fs.listStatus(path)) {
        if (file.isDirectory()) {
          messages.addAll(readMessages(file.getPath()));
        } else {
          try (SequenceFile.Reader reader = new SequenceFile.Reader(fs, file.getPath(), new Configuration())) {
            LongWritable key = (LongWritable) reader.getKeyClass().newInstance();
            Text value = (Text) reader.getValueClass().newInstance();

            while (reader.next(key, value)) {
              messages.add(gson.fromJson(value.toString(), Message.class));
            }
          }
        }
      }
    } catch (FileNotFoundException e) {
      System.out.println("No camus messages were found in [" + path + "]");
    }

    return messages;
  }

  private static class Message {

    private int number;

    // Used by Gson
    public Message() {
    }

    public Message(int number) {
      this.number = number;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null || !(obj instanceof Message))
        return false;

      Message other = (Message) obj;

      return number == other.number;
    }
  }

  private static void resetCamus() throws NoSuchFieldException, IllegalAccessException {
    // The EtlMultiOutputFormat has a static private field called committer which is only created if null. The problem is this
    // writes the Camus metadata meaning the first execution of the camus job defines where all committed output goes causing us
    // problems if you want to run Camus again using the meta data (i.e. what offsets we processed). Setting it null here forces
    // it to re-instantiate the object with the appropriate output path

    Field field = EtlMultiOutputFormat.class.getDeclaredField("committer");
    field.setAccessible(true);
    field.set(null, null);
  }

}
