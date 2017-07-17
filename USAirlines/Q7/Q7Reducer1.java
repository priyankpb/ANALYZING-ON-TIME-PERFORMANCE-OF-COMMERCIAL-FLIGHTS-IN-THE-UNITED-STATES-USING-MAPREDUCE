/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package USAirlines.Q7;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author priyankb
 */
public class Q7Reducer1 extends Reducer<Text, Q7MapperOutput, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Q7MapperOutput> mapperOutputs, Context context) throws IOException, InterruptedException {
        String airport = key.toString();
        int totLateArrivalDelay = 0;
        int totDelay = 0;
        int totFlights = 0;
        String airportName = "";
        for (Q7MapperOutput next : mapperOutputs) {
            String airport1 = next.getAirport().toString();
//            airportName = next.getAirportName().toString();

            int lateArrivalDelay = next.getLateArrivalDelay().get();
            int numFlights = next.getNumFlights().get();
            int delay = next.getDelay().get();

            if (lateArrivalDelay == -1 && numFlights == -1 && delay == -1) {
                airportName = next.getAirportName().toString();
            } else {
                totLateArrivalDelay += lateArrivalDelay;
                totFlights += numFlights;
                totDelay += delay;
            }
        }
        String tempResult = airport + "#" + airportName + "#" + totLateArrivalDelay + "#" + totDelay + "#" + totFlights;
        Text key2 = new Text();
        key2.set("KEY");

        Text result = new Text();
        result.set(tempResult);

        context.write(key2, result);
    }
}
