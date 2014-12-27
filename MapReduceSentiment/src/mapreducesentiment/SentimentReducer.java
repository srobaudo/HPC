/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mapreducesentiment;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author camila
 */
public class SentimentReducer extends Reducer<SentimentKeyWritableComparable, LongWritable, SentimentKeyWritableComparable, IntWritable> {
    private IntWritable result = new IntWritable();
 
    @Override
   public void reduce(SentimentKeyWritableComparable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
     int sum = 0;
     for (LongWritable val : values) {
       sum += val.get();
     }
     result.set(sum);
     context.write(key, result);
   } 
}
