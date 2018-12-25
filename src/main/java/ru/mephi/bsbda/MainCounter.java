package ru.mephi.bsbda;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class MainCounter extends Configured implements Tool {

    //@param args first and second args takes input files, third param take output file
    public static void main(final String args[]) throws Exception {
        Configuration conf = new Configuration();
        int res = ToolRunner.run(conf, new MainCounter(), args);
        System.exit(res);
    }

    //Run method which schedules the Hadoop Job
    //@param args Arguments passed in main function
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.output.textoutputformat.separator", ";");

        Job job = Job.getInstance(conf, "MapReduce JOB");

        //Job
        job.setJarByClass(MainCounter.class);
        job.setReducerClass(MainReducer.class);

        //Map
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LogInfo.class);

        //Reduce
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(5);

        //Input files
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MainMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, MainMapper2.class);

        //Output file
        Path outputPath = new Path(args[2]);
        FileOutputFormat.setOutputPath(job, outputPath);
        job.setOutputFormatClass(TextOutputFormat.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }
}