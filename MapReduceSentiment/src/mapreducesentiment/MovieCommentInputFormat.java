/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mapreducesentiment;

import java.io.DataInput;
import java.io.IOException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.LineReader;
 
public class MovieCommentInputFormat extends InputFormat<SentimentKeyWritableComparable, Text>{
 
    public Pattern pattern=Pattern.compile("product\\/productId\\:\\s+([a-zA-Z0-9]+).*review\\/score\\:\\s+([\\d\\.]+).*review\\/text\\:\\s+(.*)",
                Pattern.CASE_INSENSITIVE | Pattern.DOTALL | Pattern.MULTILINE);
    private final TextInputFormat textIF = new TextInputFormat();
 
     
@Override
public List<InputSplit> getSplits(JobContext context) throws IOException,
        InterruptedException {
     
    // Toma en cuenta la cantidad de mappers para dividir los splits
    return textIF.getSplits(context);
}
 
 
@Override
public RecordReader<SentimentKeyWritableComparable, Text> createRecordReader(
        InputSplit split, TaskAttemptContext context) throws IOException,
        InterruptedException {
        
       RegexRecordReader reader = new RegexRecordReader();
 
        if (pattern == null) {
          throw new IllegalStateException(
              "No pattern specified - unable to create record reader");
        }
 
        reader.setPattern(pattern);
        return reader;
}
 
 
public static class RegexRecordReader extends RecordReader<SentimentKeyWritableComparable, Text> {
 
    private final LineRecordReader lineRecordReader = new LineRecordReader();
    private Pattern pattern;
    SentimentKeyWritableComparable key = new SentimentKeyWritableComparable();
    Text value = new Text();
    private LineReader in;
    private final static Text EOL = new Text("\n");
 
    public void setPattern(Pattern pattern2) {
        // TODO Auto-generated method stub
        pattern=pattern2;
    }
 
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        // TODO Auto-generated method stub
        lineRecordReader.initialize(split, context);
    }
 
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        // TODO Auto-generated method stub
     
        String comentario = new String();
           while(lineRecordReader.nextKeyValue() && !"".equals(lineRecordReader.getCurrentValue().toString())) { //no se si "" o "\n"
            comentario = comentario.concat(lineRecordReader.getCurrentValue().toString()); //hay que ver que pasa con los "\n"
            comentario = comentario.concat("\n");
           }
           
           Matcher matcher;
 
            matcher = pattern.matcher(comentario);

            if (matcher.find()) {
              int fieldCount;
              Text[] fields;

              fieldCount = matcher.groupCount();
              fields = new Text[fieldCount];

              for (int i = 0; i < fieldCount; i++) {
                fields[i] = new Text(matcher.group(i + 1));
              }
              
              key.setNumberComment(Integer.parseInt(fields[1].toString()));
              key.setScore(Double.parseDouble(fields[2].toString()));
              value.set(fields[3]);
              return true;
            }             
            return false;
    }
 
    @Override
    public SentimentKeyWritableComparable getCurrentKey() throws IOException,
            InterruptedException {
        // TODO Auto-generated method stub
        return key;
    }
 
    @Override
    public Text getCurrentValue() throws IOException,
            InterruptedException {
        // TODO Auto-generated method stub
        //System.out.print(value);
        return value;
    }
 
    @Override
    public float getProgress() throws IOException, InterruptedException {
        // TODO Auto-generated method stub
        return lineRecordReader.getProgress();
    }
 
    @Override
    public void close() throws IOException {
        // TODO Auto-generated method stub
        lineRecordReader.close();
    }
    }
}
