package org.formation.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class RecordReaderFile extends RecordReader<Text, BytesWritable> {

    private BytesWritable value = new BytesWritable();
    private Text key = new Text();
    private Configuration conf;
    boolean done = false;
    FileSplit split;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        this.split = (FileSplit) inputSplit;
        conf = taskAttemptContext.getConfiguration();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        boolean oldDone = done;
        if (!done) {
            Path path = split.getPath();
            key.set(path.toString());
            try (FSDataInputStream fsDataInputStream = FileSystem.get(conf).open(path)) {
                byte[] buffer = new byte[(int) split.getLength()];
                fsDataInputStream.readFully(0, buffer);
                value.set(buffer, 0, buffer.length);
            }
            done = true;
        }
        return !oldDone;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {}
}
