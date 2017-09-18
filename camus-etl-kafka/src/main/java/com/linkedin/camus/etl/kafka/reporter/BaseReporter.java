package com.linkedin.camus.etl.kafka.reporter;

import java.util.Map;
import java.io.IOException;

import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskReport;


public abstract class BaseReporter {
  public static org.apache.log4j.Logger log;

  public BaseReporter() {
    this.log = org.apache.log4j.Logger.getLogger(BaseReporter.class);
  }

  public abstract void report(TaskReport[] tasks, Counters counters, Job job, Map<String, Long> timingMap) throws IOException;
}
