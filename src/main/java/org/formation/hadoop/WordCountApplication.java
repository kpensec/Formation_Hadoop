package org.formation.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
//import org.apache.hadoop.


public class WordCountApplication
{
    public static void main( String[] args )
    {
        //Configuration conf = new Configuration();
        try {
            System.out.println("arg 0 -> " + args[0]);
            System.out.println("arg 1 -> " + args[1]);
            System.out.println("arg 1 -> " + args[2]);

            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.newInstance(conf);
            Path inputFilePath = new Path(args[0]);
            Path outputFilePath = new Path(args[1]);

            if (!fs.exists(outputFilePath)) {
                Job job = new Job(conf);
                job.setJobName("Word count");

                job.setJarByClass(WordCountApplication.class);
                job.setReducerClass(Reduce.class);
                job.setCombinerClass(Reduce.class);
                job.setMapperClass(Map.class);

                job.setOutputFormatClass(TextOutputFormat.class);
                job.setInputFormatClass(TextInputFormat.class);
                job.setOutputKeyClass(org.apache.hadoop.io.Text.class);
                job.setOutputValueClass(org.apache.hadoop.io.IntWritable.class);

                FileInputFormat.addInputPath(job, inputFilePath);
                FileOutputFormat.setOutputPath(job, outputFilePath);

                job.waitForCompletion(true);
                // fs.delete(outputFilePath, true);
            }



            Job sortingJob = new Job(conf);
            sortingJob.setJarByClass(WordCountApplication.class);
            sortingJob.setMapperClass(MapSwitcher.class);
            sortingJob.setReducerClass(Reducer.class);

            sortingJob.setOutputFormatClass(TextOutputFormat.class);
            sortingJob.setInputFormatClass(TextInputFormat.class);
            sortingJob.setOutputKeyClass(org.apache.hadoop.io.IntWritable.class);
            sortingJob.setOutputValueClass(org.apache.hadoop.io.Text.class);


            inputFilePath = new Path(args[1]+"/part-r-00000");
            outputFilePath = new Path(args[2]);
            FileInputFormat.addInputPath(sortingJob, inputFilePath);
            FileOutputFormat.setOutputPath(sortingJob, outputFilePath);

            if (fs.exists(outputFilePath)) {
                fs.delete(outputFilePath, true);
            }

            sortingJob.waitForCompletion(true);


        } catch (Exception ee) {
            ee.printStackTrace();
        }
        System.out.println("Done");
    }
}
