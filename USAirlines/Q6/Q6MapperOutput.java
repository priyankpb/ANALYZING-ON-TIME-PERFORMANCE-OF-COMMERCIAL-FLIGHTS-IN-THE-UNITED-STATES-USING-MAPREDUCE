/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package USAirlines.Q6;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author priyankb
 */
public class Q6MapperOutput implements Writable {

    BooleanWritable planeData;
    IntWritable year;
    IntWritable delay;
    IntWritable numFlights;

    public Q6MapperOutput() {
        planeData = new BooleanWritable();
        year = new IntWritable();
        delay = new IntWritable();
        numFlights = new IntWritable();
    }

    public Q6MapperOutput(BooleanWritable planeData, IntWritable year, IntWritable delay, IntWritable numFlights) {
        this.planeData = planeData;
        this.year = year;
        this.delay = delay;
        this.numFlights = numFlights;
    }

    public BooleanWritable getPlaneData() {
        return planeData;
    }

    public void setPlaneData(BooleanWritable planeData) {
        this.planeData = planeData;
    }

    public IntWritable getYear() {
        return year;
    }

    public void setYear(IntWritable year) {
        this.year = year;
    }

    public IntWritable getDelay() {
        return delay;
    }

    public void setDelay(IntWritable delay) {
        this.delay = delay;
    }

    public IntWritable getNumFlights() {
        return numFlights;
    }

    public void setNumFlights(IntWritable numFlights) {
        this.numFlights = numFlights;
    }

    @Override
    public void write(DataOutput d) throws IOException {
        planeData.write(d);
        year.write(d);
        delay.write(d);
        numFlights.write(d);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        planeData.readFields(di);
        year.readFields(di);
        delay.readFields(di);
        numFlights.readFields(di);
    }

}
