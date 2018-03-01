package org.formation.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    private String maxKey = "";
    private int maxValue = 0;

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        if (sum >= maxValue) {
            maxKey = key.toString();
            maxValue = sum;
        }

    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
        IntWritable value = new IntWritable(this.maxValue);
        Text key = new Text(maxKey);
        context.write(key, value);
    }
}
