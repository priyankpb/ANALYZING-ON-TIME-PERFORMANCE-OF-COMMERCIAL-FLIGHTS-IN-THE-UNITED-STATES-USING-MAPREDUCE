/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package USAirlines.Q3;

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
public class Q3MapperOutput implements Writable {

    IntWritable year;
    Text airport;
    Text airportName;
    IntWritable numFlights;

    public Q3MapperOutput() {
        year = new IntWritable();
        airport = new Text();
        airportName = new Text();
        numFlights = new IntWritable();
    }

    public IntWritable getYear() {
        return year;
    }

    public void setYear(IntWritable year) {
        this.year = year;
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

    @Override
    public void write(DataOutput d) throws IOException {
        year.write(d);
        airport.write(d);
        airportName.write(d);
        numFlights.write(d);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        year.readFields(di);
        airport.readFields(di);
        airportName.readFields(di);
        numFlights.readFields(di);
    }

}
