package org.formation.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class MapSwitcher extends Mapper<Object, Text, IntWritable, Text> {
    Text resValue = new Text();
    IntWritable resKey = new IntWritable();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer strTok = new StringTokenizer(value.toString());

        try {
            String tmpValue = strTok.nextToken();
            Integer tmpKey = Integer.parseInt(strTok.nextToken());

            resValue.set(tmpValue);
            resKey.set(tmpKey);
            context.write(resKey, resValue);
        } catch(Exception ee) {
            ee.printStackTrace();
        }
    }
}
