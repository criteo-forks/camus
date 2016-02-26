package com.linkedin.camus.etl.kafka.common;

import com.google.common.base.Objects;

public class Source {

  private long count;
  private long start;
  private String service;
  private String server;

  public Source(String server, String service, long monitorGranularity) {
    this.server = server;
    this.service = service;
    this.start = monitorGranularity;
  }

  public Source() {

  }

  public long getCount() {
    return count;
  }

  public String getServer() {
    return server;
  }

  public String getService() {
    return service;
  }

  public long getStart() {
    return start;
  }

  public void setCount(long count) {
    this.count = count;
  }

  public void setServer(String server) {
    this.server = server;
  }

  public void setService(String service) {
    this.service = service;
  }

  public void setStart(long start) {
    this.start = start;
  }

  public void increment(long inc) {
    this.count += inc;
  }

  public void increment() {
    this.increment(1);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(server, service, start);
  }

  @Override
  public boolean equals(Object obj) {
    if(obj == null){
      return false;
    } else {
      if( obj instanceof Source){
        Source other = (Source) obj;
        return Objects.equal(this.server, other.server) && Objects.equal(this.service, other.service) && this.start == other.start;
      } else {
        return false;
      }
    }
  }

  public static String toString(String server, String service, long start) {
    StringBuilder sb = new StringBuilder('{');
    sb.append(server).append(',').append(service).append(',').append(start).append('}');
    return sb.toString();
  }

  @Override
  public String toString() {
    return toString(server, service, start);
  }

}
