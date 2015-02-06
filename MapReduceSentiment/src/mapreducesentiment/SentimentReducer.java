/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mapreducesentiment;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author camila
 */
public class SentimentReducer extends Reducer<SentimentKeyWritableComparable, LongWritable, SentimentKeyWritableComparable, DoubleWritable> {

    private DoubleWritable result = new DoubleWritable();

    @Override
    public void reduce(SentimentKeyWritableComparable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        
        System.out.println("Entro al reducer");
        Double sum = 0.0;
        long count = 0;
        
        for (LongWritable val : values) {
            //System.out.println("Value: " + val.get());
            sum += getScore(key.getScore(), val.get());
            count++;
        }
                
        result.set(sum / count);
        context.write(key, result);
    }
    
    private double getScore(DoubleWritable expected, Long got)
    {
        Long distance = Math.abs(got - Math.round(expected.get()));
        return distance == 0 
                ? 1.0 
                : distance == 1 
                    ? 0.75 
                    : 0.0;
    }
}
