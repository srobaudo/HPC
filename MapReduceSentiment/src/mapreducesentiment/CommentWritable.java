/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mapreducesentiment;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author camila
 */
public class CommentWritable  implements Writable{
   
       // Some data     
       private double score;
       private String comment;
       
       @Override
       public void write(DataOutput out) throws IOException {
         out.writeDouble(score);
         out.writeBytes(comment);
       }
       
       @Override
       public void readFields(DataInput in) throws IOException {
            score = in.readDouble();
            String res = "";                    
            String line;
            while ((line = in.readLine()) != null) {
                res = res.concat(line);                       
            }
            comment = res;
       }
       
       public static CommentWritable read(DataInput in) throws IOException {
         CommentWritable w = new CommentWritable();
         w.readFields(in);
         return w;
       }
    
}
