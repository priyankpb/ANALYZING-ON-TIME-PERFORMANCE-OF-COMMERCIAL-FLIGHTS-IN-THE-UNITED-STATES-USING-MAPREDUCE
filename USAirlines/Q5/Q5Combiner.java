/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package USAirlines.Q5;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author priyankb
 */
public class Q5Combiner extends Reducer<Text, Q5MapperOutput, Text, Q5MapperOutput> {

    @Override
    protected void reduce(Text key, Iterable<Q5MapperOutput> mapperOutputs, Context context) throws IOException, InterruptedException {
        String carrier = key.toString();
        int totDelay = 0;
        int totFlights = 0;
        for (Q5MapperOutput next : mapperOutputs) {
            int delay = next.getDelay().get();
            int flights = next.getNumFlights().get();

            if (delay == -1 && flights == -1) {
                context.write(key, next);
            } else {
                totDelay += delay;
                totFlights += flights;
            }
        }
        
        Q5MapperOutput mapperOutput = new Q5MapperOutput();
        mapperOutput.setCarrier(new Text(carrier));
        mapperOutput.setCarrierName(new Text("X"));
        mapperOutput.setDelay(new IntWritable(totDelay));
        mapperOutput.setNumFlights(new IntWritable(totFlights));
        
        context.write(key, mapperOutput);
        
    }
}
