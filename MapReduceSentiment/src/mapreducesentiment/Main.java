/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mapreducesentiment;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author cami
 */
public class Main extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Main(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = new Job(new Configuration(), "sentiment");

        job.setOutputKeyClass(SentimentKeyWritableComparable.class);
        job.setOutputValueClass(LongWritable.class);

        job.setMapperClass(SentimentMapper.class);
        job.setReducerClass(SentimentReducer.class);

        job.setInputFormatClass(MovieCommentInputFormat.class);

        FileInputFormat.setInputPaths(job, new Path("wasb:///Scene.main.mdl"));//args[0]));
        FileOutputFormat.setOutputPath(job, new Path("wasb:///sentiment/test/output"));//args[1]));

        job.setJarByClass(Main.class);

        job.submit();
        return 0;
    }
}
