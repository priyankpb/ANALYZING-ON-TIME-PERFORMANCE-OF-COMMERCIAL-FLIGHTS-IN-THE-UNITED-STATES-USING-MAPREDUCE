/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package USAirlines.Q3;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author priyankb
 */
public class Q3Reducer1 extends Reducer<Text, Q3MapperOutput, Text, Text> {

    Map<String, Integer> data = new HashMap();

    @Override
    protected void reduce(Text key, Iterable<Q3MapperOutput> mapperOutputs, Context context) throws IOException, InterruptedException {

        String year = key.toString();
        String airportName = "";
        Text key2 = new Text();
        key2.set("KEY");

        for (Q3MapperOutput mapperOutput : mapperOutputs) {

            int year2 = mapperOutput.getYear().get();
            String airport = mapperOutput.getAirport().toString();
            int numFlights = mapperOutput.getNumFlights().get();

            if (year2 == -1 && numFlights == -1) {
                airportName = mapperOutput.getAirportName().toString();
                String tempResult = airport + "#" + airportName;
                Text result = new Text();
                result.set(tempResult);
                context.write(key2, result);
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

        boolean valid = isNumeric(year);

        if (valid) {
            String tempResult = year + "#";

            for (Map.Entry<String, Integer> entrySet : data.entrySet()) {
                String airport = entrySet.getKey();
                int totFlights = entrySet.getValue();

                tempResult += airport + "@" + totFlights + "%";

            }

            Text result = new Text();
            result.set(tempResult);

            context.write(key2, result);
        }
    }

    public static boolean isNumeric(String str) {
        try {
            int d = Integer.parseInt(str);
        } catch (NumberFormatException nfe) {
            return false;
        }
        return true;
    }

}
