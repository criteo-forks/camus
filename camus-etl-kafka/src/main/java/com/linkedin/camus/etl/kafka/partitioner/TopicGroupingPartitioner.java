package com.linkedin.camus.etl.kafka.partitioner;

import org.apache.hadoop.mapred.TaskAttemptContext;
import org.apache.hadoop.mapreduce.JobContext;


public class TopicGroupingPartitioner extends DefaultPartitioner {

  @Override
  public String getWorkingFileName(JobContext context, String topic, int partitionId, String encodedPartition) {
    //adding taskId to filename to avoid name collision
    int taskId = ((TaskAttemptContext) context).getTaskAttemptID().getTaskID().getId();
    return super.getWorkingFileName(context, topic, taskId, encodedPartition);
  }

  @Override
  public String generateFileName(JobContext context, String topic, int partitionId, int count, long offset,
                                 String encodedPartition) {
    //adding taskId to filename to avoid name collision
    int taskId = ((TaskAttemptContext) context).getTaskAttemptID().getTaskID().getId();
    return super.generateFileName(context, topic, taskId, count, 0, encodedPartition);
  }

}
