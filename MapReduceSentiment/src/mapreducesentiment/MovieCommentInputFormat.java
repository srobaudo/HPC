/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mapreducesentiment;

import java.io.IOException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class MovieCommentInputFormat extends InputFormat<SentimentKeyWritableComparable, Text> {

    public static final String START_TAG_KEY = "productId";
    public static final String END_TAG_KEY = "\n\n";
    public static final String COMPLETE_START_TAG_KEY = "product/productId";
    private final TextInputFormat textIF = new TextInputFormat();
    public Pattern pattern = Pattern.compile("product\\/productId\\:\\s+([a-zA-Z0-9]+).*review\\/score\\:\\s+([\\d\\.]+).*review\\/text\\:\\s+(.*)",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL | Pattern.MULTILINE);

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
        // Toma en cuenta la cantidad de mappers para dividir los splits o el tamaño máximo de split
        return textIF.getSplits(context);
    }
    
    @Override
    public RecordReader<SentimentKeyWritableComparable, Text> createRecordReader(InputSplit is, TaskAttemptContext context) throws IOException, InterruptedException {
        TaggedRegexRecordReader reader = new TaggedRegexRecordReader();

        if (pattern == null) {
            throw new IllegalStateException(
                    "No pattern specified - unable to create record reader");
        }

        reader.setPattern(pattern);
        return reader;
    }

    public static class TaggedRegexRecordReader extends RecordReader<SentimentKeyWritableComparable, Text> {

        private byte[] startTag;
        private byte[] endTag;
        private static byte[] completeStartTag;
        private long start;
        private long end;
        private FSDataInputStream fsin;
        private Pattern pattern;
        SentimentKeyWritableComparable key = new SentimentKeyWritableComparable();
        Text value = new Text();
        private final DataOutputBuffer buffer = new DataOutputBuffer();

        public TaggedRegexRecordReader() throws IOException {            
        }

        public void setPattern(Pattern pattern2) {
            pattern = pattern2;
        }

        @Override
        public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException, InterruptedException {
            startTag = START_TAG_KEY.getBytes("utf-8");
            endTag =  new byte[] {13, 10, 13, 10};// ES \n\n
            completeStartTag = COMPLETE_START_TAG_KEY.getBytes("utf-8");
            

            FileSplit split = (FileSplit)genericSplit;
            // open the file and seek to the start of the split
            start = split.getStart();
            end = start + split.getLength();
            System.out.println("start : "+ start);
            System.out.println("end: "+ end);
            Path file = split.getPath();
            Configuration job = context.getConfiguration();
            FileSystem fs = file.getFileSystem(job);
            fsin = fs.open(split.getPath());
            fsin.seek(start);
            
            System.out.println("Terminó de inicializar input format");
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
             if (fsin.getPos() < end) {
                if (readUntilMatch(startTag, false)) {
                    try {
                        buffer.write(completeStartTag);
                        if (readUntilMatch(endTag, true)) {
                            //Encontró un cuerpo de comentario
                            Matcher matcher;
                            String comentario = new String(buffer.getData());
                            matcher = pattern.matcher(comentario);
                            if (matcher.find()) {
                                //Matcheo la expresión regular que parsea el texto escrito por el usuario
                                int fieldCount;
                                Text[] fields;

                                fieldCount = matcher.groupCount();
                                fields = new Text[fieldCount];

                                for (int i = 0; i < fieldCount; i++) {
                                    fields[i] = new Text(matcher.group(i + 1));
                                    System.out.println("Field "+ i +" - " + fields[i]);
                                }

                                key.setProductId(new Text(fields[0].toString())); 
                                key.setScore(new DoubleWritable(Double.parseDouble(fields[1].toString())));
                                
                                value.set(fields[2]);
                                return true;
                            }
                            return false;
                        }
                    }catch(Exception ex){
                        System.out.println("ERROR: " + ex.getLocalizedMessage());
                    } 
                    finally {
                        buffer.reset();
                    }
                }
            }
            return false;
        }

        @Override
        public SentimentKeyWritableComparable getCurrentKey() throws IOException,
                InterruptedException {
            return key;
        }

        @Override
        public Text getCurrentValue() throws IOException,
                InterruptedException {
            return value;
        }

        @Override
        public void close() throws IOException {
            fsin.close();
        }

        @Override
        public float getProgress() throws IOException {
            return ((fsin.getPos() - start) / (float) (end - start));
        }

        private boolean readUntilMatch(byte[] match, boolean withinBlock) throws IOException {
            int i = 0;
            while (true) {
                int b = fsin.read();
                // end of file:
                if (b == -1) {
                    //Se terminó el archivo
                    return false;
                }
                // save to buffer:
                if (withinBlock) {
                    buffer.write(b);
                }

                // check if we're matching:
                if (b == match[i]) {
                    i++;
                    if (i >= match.length) {
                        //Enccontro el tag buscado
                        return true;
                    }
                } else {
                    i = 0;
                }
                // see if we've passed the stop point:
                if (!withinBlock && i == 0 && fsin.getPos() >= end) {
                     //No encontró el tag buscado y se paso del end del split
                    return false;
                }
            }
        }

    }
}
