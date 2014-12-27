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
public class SentimentKeyWritableComparable implements WritableComparable {

    // Some data

    private Double score;
    private Integer productId;

    public void setScore(Double _score) {
        score = _score;
    }

    public void setProductId(Integer _numComment) {
        productId = _numComment;
    }

    public Double getScore() {
        return score;
    }

    public Integer getProductId() {
        return productId;
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(productId);
        out.writeDouble(score);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        productId = in.readInt();
        score = in.readDouble();
    }

    @Override
    public int compareTo(Object o) {
        SentimentKeyWritableComparable sentiment = (SentimentKeyWritableComparable) o;
        Integer thisValue = this.productId;
        Integer thatValue = sentiment.productId;

        return this.equals(o) ? 0 : (thatValue >= thisValue ? -1 : 1);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + productId;
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
}
