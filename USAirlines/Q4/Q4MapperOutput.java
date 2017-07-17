/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package USAirlines.Q4;

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
public class Q4MapperOutput implements Writable {

    Text airport;
    Text city;
    IntWritable weatherDelay;
    IntWritable records;

    public Q4MapperOutput() {
        airport = new Text();
        city = new Text();
        weatherDelay = new IntWritable();
        records = new IntWritable();
    }

    public Text getAirport() {
        return airport;
    }

    public void setAirport(Text airport) {
        this.airport = airport;
    }

    public Text getCity() {
        return city;
    }

    public void setCity(Text city) {
        this.city = city;
    }

    public IntWritable getWeatherDelay() {
        return weatherDelay;
    }

    public void setWeatherDelay(IntWritable weatherDelay) {
        this.weatherDelay = weatherDelay;
    }

    public IntWritable getRecords() {
        return records;
    }

    public void setRecords(IntWritable records) {
        this.records = records;
    }

    @Override
    public void write(DataOutput d) throws IOException {
        airport.write(d);
        city.write(d);
        weatherDelay.write(d);
        records.write(d);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        airport.readFields(di);
        city.readFields(di);
        weatherDelay.readFields(di);
        records.readFields(di);
    }

}
