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
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 *
 * @author camila
 */
public class SentimentKeyWritableComparable implements WritableComparable {

    // Some data

    private final DoubleWritable score = new DoubleWritable();
    private final Text productId = new Text();

    public void setScore(DoubleWritable _score) {
        score.set(_score.get());
    }

    public void setProductId(Text _numComment) {
        productId.set(_numComment);
    }

    public DoubleWritable getScore() {
        return score;
    }

    public Text getProductId() {
        return productId;
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
        System.out.println("WRITE KEY PRODUCTID - " + productId.toString());
        System.out.println("WRITE KEY SCORE - " + score.toString());
        productId.write(out);
        score.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        productId.readFields(in);
        score.readFields(in);
    }

    @Override
    public int compareTo(Object o) {
        SentimentKeyWritableComparable sentiment = (SentimentKeyWritableComparable) o;
        Text thisValue = this.productId;
        Text thatValue = sentiment.productId;

        return this.equals(o) ? 0 : (thatValue.compareTo(thisValue) == 0 ? -1 : 1);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + productId.hashCode();
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
        return Objects.equals(this.productId, other.productId)
                && Objects.equals(this.score, other.score);
    }

    @Override
    public String toString() {
        return productId.toString() + "-" + score.toString();  //To change body of generated methods, choose Tools | Templates.
    }
    
    
}
