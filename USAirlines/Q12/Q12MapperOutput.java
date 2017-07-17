/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package USAirlines.Q12;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author priyankb
 */
public class Q12MapperOutput implements Writable {

    IntWritable delay;
    IntWritable sum;

    public Q12MapperOutput() {
        this.delay = new IntWritable();
        this.sum = new IntWritable();
    }

    public IntWritable getDelay() {
        return delay;
    }

    public void setDelay(IntWritable delay) {
        this.delay = delay;
    }

    public IntWritable getSum() {
        return sum;
    }

    public void setSum(IntWritable sum) {
        this.sum = sum;
    }

    
    
    @Override
    public void write(DataOutput d) throws IOException {
        delay.write(d);
        sum.write(d);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        delay.readFields(di);
        sum.readFields(di);
    }

}
