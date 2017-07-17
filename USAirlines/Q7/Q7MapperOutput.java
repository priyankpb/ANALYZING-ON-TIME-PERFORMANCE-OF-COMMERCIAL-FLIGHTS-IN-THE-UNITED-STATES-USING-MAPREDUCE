/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package USAirlines.Q7;

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
public class Q7MapperOutput implements Writable {

    IntWritable lateArrivalDelay;
    IntWritable delay;
    Text airport;
    Text airportName;
    IntWritable numFlights;

    public Q7MapperOutput() {
        lateArrivalDelay = new IntWritable();
        delay=new IntWritable();
        airport = new Text();
        airportName = new Text();
        numFlights = new IntWritable();
    }

    public IntWritable getLateArrivalDelay() {
        return lateArrivalDelay;
    }

    public void setLateArrivalDelay(IntWritable lateArrivalDelay) {
        this.lateArrivalDelay = lateArrivalDelay;
    }

    public Text getAirport() {
        return airport;
    }

    public void setAirport(Text airport) {
        this.airport = airport;
    }

    public Text getAirportName() {
        return airportName;
    }

    public void setAirportName(Text airportName) {
        this.airportName = airportName;
    }

    public IntWritable getNumFlights() {
        return numFlights;
    }

    public void setNumFlights(IntWritable numFlights) {
        this.numFlights = numFlights;
    }

    public IntWritable getDelay() {
        return delay;
    }

    public void setDelay(IntWritable delay) {
        this.delay = delay;
    }

    @Override
    public void write(DataOutput d) throws IOException {
        lateArrivalDelay.write(d);
        delay.write(d);
        airport.write(d);
        airportName.write(d);
        numFlights.write(d);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        lateArrivalDelay.readFields(di);
        delay.readFields(di);
        airport.readFields(di);
        airportName.readFields(di);
        numFlights.readFields(di);
    }

}
