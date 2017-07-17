/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package USAirlines.Q5;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author priyankb
 */
public class Q5Reducer1 extends Reducer<Text, Q5MapperOutput, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Q5MapperOutput> mapperOutputs, Context context) throws IOException, InterruptedException {
        String carrier = key.toString();
        String carrierName = "";
        int totDelay = 0;
        int totFlights = 0;
        for (Q5MapperOutput next : mapperOutputs) {
            int delay = next.getDelay().get();
            int flights = next.getNumFlights().get();
            if (delay == -1 && flights == -1) {
                carrierName = next.getCarrierName().toString();
            } else {
                totDelay += delay;
                totFlights += flights;
            }
        }
        String tempResult = carrier + "#" + carrierName + "#" + totDelay + "#" + totFlights;

        Text key2 = new Text();
        key2.set("KEY");

        Text result = new Text();
        result.set(tempResult);

        context.write(key2, result);
    }
}
