/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package USAirlines.Q3;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeMap;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import util.Protocol;

/**
 *
 * @author priyankb
 */
public class Q3Reducer2 extends Reducer<Text, Text, Text, Text> {

    Map<Integer, Map<String, Integer>> data = new TreeMap<>();
    Map<String, Integer> aggData = new TreeMap<>();
//    Map<String, Integer> sortedAggData = new LinkedHashMap<>();
    List<String> bairports = new LinkedList<>();
    Map<String, String> airports = new HashMap<>();

    @Override
    protected void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {

        for (Text result : value) {
            String decide = result.toString();
            if (decide.contains("%")) {
                String tempResult[] = decide.split("#");
                int year = Integer.parseInt(tempResult[0]);
                String ports[] = tempResult[1].split("%");

                Map<String, Integer> airportsMap = new TreeMap<>();

                for (String airportFlights : ports) {
                    String[] tempAirportFlights = airportFlights.split("@");
                    String airport = tempAirportFlights[0];
                    int numFlights = Integer.parseInt(tempAirportFlights[1]);
                    airportsMap.put(airport, numFlights);

                    if (!aggData.containsKey(airport)) {
                        aggData.put(airport, numFlights);
                    } else {
                        int flights = aggData.get(airport);
                        flights += numFlights;
                        aggData.put(airport, flights);
                    }
                }

                data.put(year, airportsMap);
            } else {
                String tempResult[] = decide.split("#");
                if (tempResult.length == 2) {
                    String airportCode = tempResult[0];
                    String airportName = tempResult[1];
                    airports.put(airportCode, airportName);
                }
            }
        }

        SortedSet<Map.Entry<String, Integer>> sortedSet = Protocol.sortMapByValue(aggData);

        context.write(new Text("---Q3---"), null);
        context.write(null, new Text("\n"));
        context.write(null, new Text("Bussiest Airports: "));

        for (Map.Entry<String, Integer> next : sortedSet) {
            bairports.add(next.getKey());
        }

        int totalAirports = bairports.size();

        for (int i = 1; i <= 10; i++) {
            String airportCode = bairports.get(totalAirports - i);
            String airportName = airports.get(airportCode);
            context.write(null, new Text(i + ". " + airportCode + " - " + airportName));
        }

        for (Map.Entry<Integer, Map<String, Integer>> entrySet : data.entrySet()) {
            int year = entrySet.getKey();
            Map<String, Integer> yairports = entrySet.getValue();
            List<String> sortedyairports = new LinkedList<>();
            context.write(null, new Text("\n----------"));
            context.write(null, new Text("---Year: " + year + "---"));
            SortedSet<Map.Entry<String, Integer>> sortedSetYearly = Protocol.sortMapByValue(yairports);

            for (Map.Entry<String, Integer> next : sortedSetYearly) {
                sortedyairports.add(next.getKey());
            }

            int totalyairports = sortedyairports.size();

            for (int i = 1; i <= 10; i++) {
                String airportCode = sortedyairports.get(totalyairports - i);
                String airportName = airports.get(airportCode);
                context.write(null, new Text(i + ". " + airportCode + " - " + airportName));
            }

        }

    }
}
