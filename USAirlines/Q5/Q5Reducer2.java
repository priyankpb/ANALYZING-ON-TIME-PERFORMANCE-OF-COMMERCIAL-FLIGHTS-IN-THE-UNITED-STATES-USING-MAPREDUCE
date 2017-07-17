/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package USAirlines.Q5;

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
public class Q5Reducer2 extends Reducer<Text, Text, Text, Text> {

    Map<String, Integer> delayMap = new HashMap();
    Map<String, Integer> flightsMap = new HashMap();
    Map<String, Float> avgDelayMap = new HashMap();

    List<String> sortedFlights = new LinkedList<>();
    List<String> sortedDelays = new LinkedList<>();
    List<String> sortedAvgDelay = new LinkedList<>();

    @Override
    protected void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
        for (Text result : value) {
            String tempResult[] = result.toString().split("#");
            String carrierCode = tempResult[0];
            String carrierName = tempResult[1];
            String carrier = carrierCode + "-" + carrierName;
            int delay = Integer.parseInt(tempResult[2]);
            int flights = Integer.parseInt(tempResult[3]);
            float avgDelay = (float) delay / flights;

            delayMap.put(carrier, delay);
            flightsMap.put(carrier, flights);
            avgDelayMap.put(carrier, avgDelay);
        }

        context.write(new Text("---Q5---"), null);
        context.write(null, new Text("\n"));
        context.write(null, new Text("Carriers with maximum delays: [Total delayed flights]"));

        SortedSet<Map.Entry<String, Integer>> sortedFlightsMap = Protocol.sortMapByValue(flightsMap);
        for (Map.Entry<String, Integer> next : sortedFlightsMap) {
            sortedFlights.add(next.getKey());
        }
        int totFlights = sortedFlights.size();
        for (int i = 1; i <= 10; i++) {
            context.write(null, new Text(i + ". " + sortedFlights.get(totFlights - i)));
        }

        context.write(null, new Text("----------"));
        context.write(null, new Text("\n"));
        context.write(null, new Text("Carriers with maximum delays: [Total number of delayed minutes]"));

        SortedSet<Map.Entry<String, Integer>> sortedDelayMap = Protocol.sortMapByValue(delayMap);
        for (Map.Entry<String, Integer> next : sortedDelayMap) {
            sortedDelays.add(next.getKey());
        }
        int totDelay = sortedDelays.size();
        for (int i = 1; i <= 10; i++) {
            context.write(null, new Text(i + ". " + sortedDelays.get(totDelay - i)));
        }

        context.write(null, new Text("----------"));
        context.write(null, new Text("\n"));
        context.write(null, new Text("Carriers with maximum delays: [Average delayed minutes]"));

        SortedSet<Map.Entry<String, Float>> sortedAvgDelayMap = Protocol.sortMapByValue(avgDelayMap);
        for (Map.Entry<String, Float> next : sortedAvgDelayMap) {
            sortedAvgDelay.add(next.getKey());
        }
        int totAvgDelay = sortedAvgDelay.size();
        for (int i = 1; i <= 10; i++) {
            context.write(null, new Text(i + ". " + sortedAvgDelay.get(totAvgDelay - i)));
        }

    }
}
