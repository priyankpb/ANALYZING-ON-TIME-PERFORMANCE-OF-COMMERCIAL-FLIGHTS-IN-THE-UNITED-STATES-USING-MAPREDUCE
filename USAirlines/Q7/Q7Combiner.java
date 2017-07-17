/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package USAirlines.Q7;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author priyankb
 */
public class Q7Combiner extends Reducer<Text, Q7MapperOutput, Text, Q7MapperOutput> {

    @Override
    protected void reduce(Text key, Iterable<Q7MapperOutput> mapperOutputs, Context context) throws IOException, InterruptedException {
        int totLateArrivalDelay = 0;
        int totDelay = 0;
        int totFlights = 0;
        String airportName = "";
        for (Q7MapperOutput next : mapperOutputs) {
            String airport = next.getAirport().toString();
            airportName = next.getAirportName().toString();

            int lateArrivalDelay = next.getLateArrivalDelay().get();
            int numFlights = next.getNumFlights().get();
            int delay = next.getDelay().get();

            if (lateArrivalDelay == -1 && numFlights == -1 && delay == -1) {
                context.write(key, next);
            } else {
                totLateArrivalDelay += lateArrivalDelay;
                totFlights += numFlights;
                totDelay += delay;
            }

        }

        Q7MapperOutput mapperOutput = new Q7MapperOutput();
        mapperOutput.setAirport(key);
        mapperOutput.setAirportName(new Text(airportName));
        mapperOutput.setLateArrivalDelay(new IntWritable(totLateArrivalDelay));
        mapperOutput.setNumFlights(new IntWritable(totFlights));
        mapperOutput.setDelay(new IntWritable(totDelay));

        context.write(key, mapperOutput);

    }
}
