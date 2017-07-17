/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package USAirlines.Q5;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author priyankb
 */
public class Q5MapperOutput implements Writable {

    IntWritable delay;
    Text carrier;
    Text carrierName;
    IntWritable numFlights;

    public Q5MapperOutput() {
        delay = new IntWritable();
        carrier = new Text();
        carrierName = new Text();
        numFlights = new IntWritable();
    }

    public IntWritable getDelay() {
        return delay;
    }

    public void setDelay(IntWritable delay) {
        this.delay = delay;
    }

    public Text getCarrier() {
        return carrier;
    }

    public void setCarrier(Text carrier) {
        this.carrier = carrier;
    }

    public Text getCarrierName() {
        return carrierName;
    }

    public void setCarrierName(Text carrierName) {
        this.carrierName = carrierName;
    }

    public IntWritable getNumFlights() {
        return numFlights;
    }

    public void setNumFlights(IntWritable numFlights) {
        this.numFlights = numFlights;
    }

    @Override
    public void write(DataOutput d) throws IOException {
        delay.write(d);
        carrier.write(d);
        carrierName.write(d);
        numFlights.write(d);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        delay.readFields(di);
        carrier.readFields(di);
        carrierName.readFields(di);
        numFlights.readFields(di);
    }

}
