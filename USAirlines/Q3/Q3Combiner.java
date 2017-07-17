/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package USAirlines.Q3;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author priyankb
 */
public class Q3Combiner extends Reducer<Text, Q3MapperOutput, Text, Q3MapperOutput> {

    Map<String, Integer> data = new HashMap();

    @Override
    protected void reduce(Text key, Iterable<Q3MapperOutput> mapperOutputs, Context context) throws IOException, InterruptedException {
        String year = key.toString();

        for (Q3MapperOutput mapperOutput : mapperOutputs) {

            int year2 = mapperOutput.getYear().get();
            String airport = mapperOutput.getAirport().toString();
            int numFlights = mapperOutput.getNumFlights().get();

            if (year2 == -1 && numFlights == -1) {
                context.write(key, mapperOutput);
            } else {
                if (!data.containsKey(airport)) {
                    data.put(airport, numFlights);
                } else {
                    Integer flights = data.get(airport);
                    flights += numFlights;
                    data.put(airport, flights);
                }
            }

        }

        for (Map.Entry<String, Integer> entrySet : data.entrySet()) {
            String airport = entrySet.getKey();
            int numFlights = entrySet.getValue();
            Q3MapperOutput mapperOutput = new Q3MapperOutput();
            mapperOutput.setYear(new IntWritable(Integer.parseInt(year)));
            mapperOutput.setAirport(new Text(airport));
            mapperOutput.setNumFlights(new IntWritable(numFlights));

            context.write(new Text(year), mapperOutput);
        }
    }
}
