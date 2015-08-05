package com.linkedin.camus.etl.kafka.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.io.SequenceFile;

/**
 * Created by m.chataigner on 8/5/15.
 */
public class EtlConfigurationUtils {
    private EtlConfigurationUtils(){
    }

    public static SequenceFile.Writer.Option smallBlockSizeOption(Configuration config) {
        return SequenceFile.Writer.blockSize(
                config.getLong(
                        DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY,
                        DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_DEFAULT));
    }

}
