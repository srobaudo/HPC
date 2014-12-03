/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package mapreducesentiment;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author Lu
 */
public class Main extends Configured implements Tool{

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Main(), args);
        System.exit(res);       
    }

    @Override
    public int run(String[] args) throws Exception {
//        if (args.length != 2) {
//            System.out.println("usage: [input] [output]");
//            System.exit(-1);
//        }

        Job job = new Job(new Configuration(), "mcell");
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);

        job.setMapperClass(SentimentMapper.class);
        //job.setReducerClass(VoteCountReducer.class);
        job.setNumReduceTasks(0);
        
        //job.setInputFormatClass(WholeFileInputFormat.class);
        //job.setOutputFormatClass(TextOutputFormat.class);

        //System.out.println(args[0]);
        
        FileInputFormat.setInputPaths(job, new Path("wasb:///Scene.main.mdl"));//args[0]));
        FileOutputFormat.setOutputPath(job, new Path("wasb:///mcell/test/output"));//args[1]));
        
        job.addCacheFile(new Path("wasb:///mcell.exe").toUri());

        job.setJarByClass(Main.class);

        job.submit();
        return 0;
    }
}
