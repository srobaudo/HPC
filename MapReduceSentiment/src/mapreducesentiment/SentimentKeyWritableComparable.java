/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mapreducesentiment;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;
import org.apache.hadoop.io.WritableComparable;

/**
 *
 * @author camila
 */
public class SentimentKeyWritableComparable  implements WritableComparable {
       // Some data
       private Double score;
       private Integer numberComment;
       
       @Override
       public void write(DataOutput out) throws IOException {         
         out.writeInt(numberComment);
         out.writeDouble(score);
       }
       
       @Override
       public void readFields(DataInput in) throws IOException {         
         numberComment = in.readInt();
         score = in.readDouble();
       }
       
       @Override
       public int compareTo(Object o) {
         SentimentKeyWritableComparable sentiment = (SentimentKeyWritableComparable) o;
         Integer thisValue = this.numberComment;
         Integer thatValue = sentiment.numberComment;
         return (thatValue >= thisValue ? -1 : (Objects.equals(thisValue, thatValue) ? 0 : 1));
       }

       @Override
       public int hashCode() {
         final int prime = 31;
         int result = 1;
         result = prime * result + numberComment;
         //result = prime * result + (int) (timestamp ^ (timestamp >>> 32));
         return result;
       }     

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final SentimentKeyWritableComparable other = (SentimentKeyWritableComparable) obj;
        if (!Objects.equals(this.numberComment, other.numberComment)) {
            return false;
        }
        return true;
    }
}
