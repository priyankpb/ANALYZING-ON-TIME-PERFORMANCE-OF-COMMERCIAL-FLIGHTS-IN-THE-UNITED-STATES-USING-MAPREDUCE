/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package USAirlines.Q4;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import util.Protocol;

/**
 *
 * @author priyankb
 */
public class Q4Reducer2 extends Reducer<Text, Text, Text, Text> {

    Map<String, Map<Float, Float>> cityMapTotal = new HashMap();
    Map<String, Float> cityMapAvg = new HashMap<>();
    Map<String, Integer> cityMap = new HashMap<>();
    List<String> cityListAvg = new LinkedList<>();
    List<String> cityList = new LinkedList<>();

    @Override
    protected void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {

        for (Text result : value) {
            String tempResult[] = result.toString().split("#");
            String city = tempResult[0];
            String airport = tempResult[1];
            float rWeatherDelay = Float.parseFloat(tempResult[2]);
            float rFlights = Float.parseFloat(tempResult[3]);

            if (cityMapTotal.containsKey(city)) {
                Map<Float, Float> get = cityMapTotal.get(city);
                float tempwd = 0;
                float tempf = 0;
                for (Map.Entry<Float, Float> entrySet : get.entrySet()) {
                    float delay = entrySet.getKey();
                    float flights = entrySet.getValue();

                    tempwd = delay + rWeatherDelay;
                    tempf = flights + rFlights;
                }

                get.clear();
                get.put(tempwd, tempf);
                cityMapTotal.put(city, get);
            } else {
                Map<Float, Float> get = new HashMap<>();
                get.put(rWeatherDelay, rFlights);
                cityMapTotal.put(city, get);
            }
        }

        for (Map.Entry<String, Map<Float, Float>> entrySet : cityMapTotal.entrySet()) {
            String city = entrySet.getKey();
            Map<Float, Float> sum = entrySet.getValue();
            for (Map.Entry<Float, Float> entrySet1 : sum.entrySet()) {
                float delay = entrySet1.getKey();
                float flights = entrySet1.getValue();

                float averageDelay = delay / flights;
                cityMap.put(city, ((int) delay));
                cityMapAvg.put(city, averageDelay);

            }
        }

        context.write(new Text("---Q4---"), null);
        context.write(null, new Text("\n"));
        context.write(null, new Text("Weather Delayed Airports: [Avg weather delay]"));

        SortedSet<Map.Entry<String, Float>> sortedSet = Protocol.sortMapByValue(cityMapAvg);

        for (Map.Entry<String, Float> next : sortedSet) {
            cityListAvg.add(next.getKey());
        }

        int totalCities = cityListAvg.size();

        for (int i = 1; i <= 10; i++) {
            context.write(null, new Text(i + ". " + cityListAvg.get(totalCities - i)));
        }
        
        SortedSet<Map.Entry<String, Integer>> sortedSet2 = Protocol.sortMapByValue(cityMap);
        for (Map.Entry<String, Integer> next : sortedSet2) {
            cityList.add(next.getKey());
        }
        
        totalCities = cityList.size();
        context.write(null, new Text("\n"));
        context.write(null, new Text("Weather Delayed Airports: [Total weather delay]"));
        
        for (int i = 1; i <= 10; i++) {
            context.write(null, new Text(i + ". " + cityList.get(totalCities - i)));
        }
    }
}
