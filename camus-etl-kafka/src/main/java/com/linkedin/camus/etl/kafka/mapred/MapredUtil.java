package com.linkedin.camus.etl.kafka.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class MapredUtil {
    private MapredUtil() {}

    public static abstract class EffectiveSeqFileReader<K extends Writable, V extends Writable>{
        public static final String CAMUS_SEQUENCE_FILE_READ_THREAD_COUNT_KEY = "camus.sequence.file.read.thread.count";
        public static final int DEFAULT_CAMUS_SEQUENCE_FILE_READ_THREAD_COUNT = 10;

        private static final Logger LOG = Logger.getLogger(EffectiveSeqFileReader.class);

        private final Configuration conf;
        private final Path path;
        private final PathFilter pathFilter;

        public EffectiveSeqFileReader(Configuration conf, Path path, PathFilter pathFilter) {
            this.conf = conf;
            this.path = path;
            this.pathFilter = pathFilter;
        }

        public abstract K createKey();
        public abstract V createValue();

        // save the current key to another object, mapreduce ideology is to reuse objects
        public abstract K copyKey(K key);
        // save the current value to another object, mapreduce ideology is to reuse objects
        public abstract V copyValue(V value);
        // save the current key to another object and add some context based on the path
        public K copyKey(K key, Path path) {
            return copyKey(key);
        }
        // save the current key to another object and add some context based on the path
        public V copyValue(V value, Path path) {
            return copyValue(value);
        }

        /**
         * Read content of SequenceFile.
         * Depending on the number of files to read, do it sequentially or in parallel.
         */
        public Map<K, V> read() throws IOException {
            FileSystem fs = FileSystem.get(path.toUri(), conf);
            FileStatus[] files = fs.listStatus(path, pathFilter);
            if (files.length > 20) {
                return readPar(files);
            } else {
                return readSeq(files);
            }
        }

        /**
         * Helper method to read full content of only one SequenceFile
         * Retry 10 times before failing
         */
        private Map<K, V> readOneFile(Path path) throws IOException {
            int retries = 10;
            int currentWait = 1000;
            while (retries > 0) {
                try {
                    Map<K, V> res = new HashMap<>();
                    Reader reader = new Reader(conf, Reader.file(path));
                    try {
                        K key = createKey();
                        V value = createValue();
                        while (reader.next(key, value)) {
                            res.put(copyKey(key, path), copyValue(value, path));
                        }
                        return res;
                    } finally {
                        reader.close();
                    }
                } catch (IOException e) {
                    LOG.error("got exception while reading " + path + " : " + e.getMessage());
                    retries--;
                    if (retries == 0) {
                        throw e;
                    } else {
                        try {
                            Thread.sleep(currentWait);
                        } catch (InterruptedException e1) {
                            LOG.error("thread interrupted while exponential backoff", e1);
                        }
                        currentWait = java.lang.Math.min(currentWait * 2, 60000);
                    }
                }
            }
            // either we return the result or throw the exception after max retries.
            throw new RuntimeException("this should not happen");
        }

        /**
         * Helper method to read all files of a SequenceFile in a sequential manner.
         */
        private Map<K, V> readSeq(FileStatus[] files) throws IOException {
            Map<K, V> res = new HashMap<>();

            for (FileStatus file : files) {
                res.putAll(readOneFile(file.getPath()));
            }

            return res;
        }


        /**
         * Helper method to read all files of a SequenceFile inside an ExecutorService in parallel
         * You can configure it with {@link EffectiveSeqFileReader#CAMUS_SEQUENCE_FILE_READ_THREAD_COUNT_KEY} config key
         */
        private Map<K, V> readPar(FileStatus[] files) throws IOException {
            final Map<K, V> res = new ConcurrentHashMap<>();

            ExecutorService es = Executors.newFixedThreadPool(conf.getInt(CAMUS_SEQUENCE_FILE_READ_THREAD_COUNT_KEY, DEFAULT_CAMUS_SEQUENCE_FILE_READ_THREAD_COUNT));
            try {
                List<Future<Void>> futures = new ArrayList<>();

                // spawn tasks in the executor service
                for (final FileStatus file : files) {
                    futures.add(es.submit(new Callable<Void>() {
                        @Override
                        public Void call() throws IOException {
                            res.putAll(readOneFile(file.getPath()));
                            return null;
                        }
                    }));
                }

                // await all tasks completion
                for (Future<Void> future : futures) {
                    future.get(2, TimeUnit.SECONDS);
                }

                return res;
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                LOG.error("something went wrong during the parallel read", e);
                return readSeq(files);
            } finally {
                es.shutdown();
            }
        }
    }
}
