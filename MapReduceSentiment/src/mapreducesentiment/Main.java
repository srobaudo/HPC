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

        //Configuración de memoria para que ejecuten 16 Maps
        conf.set("mapreduce.map.memory.mb", "1400");
        conf.set("mapreduce.reduce.memory.mb", "2800");
        conf.set("mapreduce.map.java.opts", "-Xmx1120m");
        conf.set("mapreduce.reduce.java.opts", "-Xmx2240m");
        conf.set("yarn.app.mapreduce.am.resource.mb", "2800");
        conf.set("yarn.app.mapreduce.am.command-opts", "-Xmx2240m");
        conf.set("yarn.nodemanager.resource.memory-mb", "5040");
        conf.set("yarn.scheduler.minimum-allocation-mb", "1400");
        conf.set("yarn.scheduler.maximum-allocation-mb", "5040");
        conf.set("mapreduce.task.timeout", "0");//NO timeout
        
        //Tamaño máximo de split para determinar la cantidad de splits/Mappers
        conf.set("mapreduce.input.fileinputformat.split.minsize", "0");
        conf.set("mapreduce.input.fileinputformat.split.maxsize", "104500");//total size / data nodes
        
        Job job = new Job(conf, "sentiment");

        job.setOutputKeyClass(SentimentKeyWritableComparable.class);
        job.setOutputValueClass(LongWritable.class);

        job.setMapperClass(SentimentMapper.class);
        job.setReducerClass(SentimentReducer.class);

        job.setInputFormatClass(MovieCommentInputFormat.class);
        
        
        //Archivo corpus de comentarios se lee desde el blob storage
        FileInputFormat.setInputPaths(job, new Path("wasb:///movies800K.txt"));//args[0]));
        FileOutputFormat.setOutputPath(job, new Path("wasb:///sentiment/test/movies800kb"));//args[1]));
        
        //Librerías que se copian en caché de cada data node
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
