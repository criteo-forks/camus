package com.linkedin.camus.workallocater;

import org.apache.hadoop.io.Writable;


public interface CamusRequest extends Writable {

  public abstract void setLatestOffset(long latestOffset);

  public abstract void setEarliestOffset(long earliestOffset);

  /**
   * Sets the starting offset used by the kafka pull mapper.
   * 
   * @param offset
   */
  public abstract void setOffset(long offset);

  /**
   * Retrieve the topic
   * 
   * @return
   */
  public abstract String getTopic();

  /**
   * Retrieves the partition number
   * 
   * @return
   */
  public abstract int getPartition();

  /**
   * Retrieves the offset
   * 
   * @return
   */
  public abstract long getOffset();

  /**
   * Returns true if the offset is valid (>= to earliest offset && <= to last
   * offset)
   * 
   * @return
   */
  public abstract boolean isValidOffset();

  public abstract long getEarliestOffset();

  public abstract long getLastOffset();

  public abstract long estimateDataSize();
  
  public abstract void setAvgMsgSize(long size);

}
