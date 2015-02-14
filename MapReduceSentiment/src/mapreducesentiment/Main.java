/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mapreducesentiment;

import java.util.Date;
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
        
        Configuration conf = new Configuration();
        conf.set("mapreduce.map.memory.mb", "2048");
        conf.set("mapreduce.reduce.memory.mb", "4096");
        conf.set("mapreduce.map.java.opts", "-Xmx1638m");
        conf.set("mapreduce.reduce.java.opts", "-Xmx3277m");
        conf.set("yarn.app.mapreduce.am.resource.mb", "4096");
        conf.set("yarn.app.mapreduce.am.command-opts", "-Xmx3277m");
        conf.set("yarn.nodemanager.resource.memory-mb", "4096");
        conf.set("yarn.scheduler.minimum-allocation-mb", "2048");
        conf.set("yarn.scheduler.maximum-allocation-mb", "4096");
        conf.set("mapreduce.input.fileinputformat.split.minsize", "0");
        conf.set("mapreduce.input.fileinputformat.split.maxsize", "928800");//total size / data nodes
        conf.set("mapreduce.task.timeout", "0");//NO timeout
        
       // conf.set("yarn.scheduler.minimum-allocation-mb", "100");
        //conf.set("yarn.scheduler.maximum-allocation-mb", "2000");
        
       

        Job job = new Job(conf, "sentiment");

        job.setOutputKeyClass(SentimentKeyWritableComparable.class);
        job.setOutputValueClass(LongWritable.class);

        job.setMapperClass(SentimentMapper.class);
        job.setReducerClass(SentimentReducer.class);

        job.setInputFormatClass(MovieCommentInputFormat.class);
        
        

        FileInputFormat.setInputPaths(job, new Path("wasb:///movies7mb.txt"));//args[0]));
        Date d = new Date();
        FileOutputFormat.setOutputPath(job, new Path("wasb:///sentiment/test/outputMovies4"));//args[1]));
        
        job.addCacheFile(new Path("wasb:///ejml-0.23.jar").toUri());
        job.addCacheFile(new Path("wasb:///javax.json.jar").toUri());
        job.addCacheFile(new Path("wasb:///jollyday.jar").toUri());
        job.addCacheFile(new Path("wasb:///stanford-corenlp-3.4.1.jar").toUri());
        job.addCacheFile(new Path("wasb:///stanford-corenlp-3.4.1-models.jar").toUri());
        job.addCacheFile(new Path("wasb:///xom.jar").toUri());

        job.setJarByClass(Main.class);

        job.submit();
        return 0;
    }
}
