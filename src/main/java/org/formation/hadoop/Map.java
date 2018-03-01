package org.formation.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class Map extends Mapper<Object, Text, Text, IntWritable> {
    private static final IntWritable valueOne = new IntWritable(1);
    private Text word = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] strArray = value.toString().split("\"");
        if(strArray.length > 2) {
           String[] strSubArray = strArray[1].split(" ");
           if(strSubArray.length > 2) {
               word.set(strSubArray[1]);
               context.write(word, valueOne);
           }
        }
    }
}
