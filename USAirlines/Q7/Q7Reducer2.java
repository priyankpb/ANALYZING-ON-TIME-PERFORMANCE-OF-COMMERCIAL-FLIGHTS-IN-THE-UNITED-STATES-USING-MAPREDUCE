/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package USAirlines.Q7;

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
public class Q7Reducer2 extends Reducer<Text, Text, Text, Text> {

    Map<String, Delays> delayMap = new HashMap();
    Map<String, String> airportMap = new HashMap<>();
    Map<String, Integer> lateArrivalDelayMap = new HashMap<>();
    List<String> airportList = new LinkedList<>();

    @Override
    protected void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
        for (Text result : value) {
            String tempResult[] = result.toString().split("#");
            String airport = tempResult[0];
            String airportName = tempResult[1];
            int rLateArrivalDelay = Integer.parseInt(tempResult[2]);
            int rDelay = Integer.parseInt(tempResult[4]);
            int rFlights = Integer.parseInt(tempResult[4]);

            if (!delayMap.containsKey(airport)) {
                Delays d = new Delays(rLateArrivalDelay, rDelay, rFlights);
                delayMap.put(airport, d);
            } else {
                Delays get = delayMap.get(airport);
                int lateArrivalDelay = get.lateArrivalDelay;
                int delay = get.delay;
                int flights = get.numFlights;

                lateArrivalDelay += rLateArrivalDelay;
                delay += rDelay;
                flights += rFlights;

                Delays d = new Delays(lateArrivalDelay, delay, flights);
                delayMap.put(airport, d);
            }

            if (!airportMap.containsKey(airport)) {
                airportMap.put(airport, airportName);
            }

            if (!lateArrivalDelayMap.containsKey(airport)) {
                lateArrivalDelayMap.put(airport, rLateArrivalDelay);
            } else {
                int get = lateArrivalDelayMap.get(airport);
                get += rLateArrivalDelay;
                lateArrivalDelayMap.put(airport, get);
            }
        }
        SortedSet<Map.Entry<String, Integer>> sortedSet = Protocol.sortMapByValue(lateArrivalDelayMap);
        for (Map.Entry<String, Integer> next : sortedSet) {
            airportList.add(next.getKey());
        }

        int totalAirports = airportList.size();
        context.write(new Text("---Q7---"), null);
        context.write(null, new Text("\n"));
        context.write(null, new Text("Airports: "));
        for (int i = 1; i <= 10; i++) {
            String airportCode = airportList.get(totalAirports - i);
            String airportName = airportMap.get(airportCode);
            context.write(null, new Text(i + ". " + airportCode + " - " + airportName));
        }

        context.write(null, new Text("----------"));
        context.write(null, new Text("\n"));
        context.write(null, new Text("Some Statistics:"));
        context.write(null, new Text("\tTotal Airports: " + totalAirports));
        int totLateArrivalDelay = 0;
        int totDelay = 0;
        int totflights = 0;
        for (Map.Entry<String, Delays> entrySet : delayMap.entrySet()) {
            String airport = entrySet.getKey();
            Delays d = entrySet.getValue();
            totLateArrivalDelay += d.lateArrivalDelay;
            totDelay += d.delay;
            totflights += d.numFlights;
        }

        totLateArrivalDelay /= 2;

        context.write(null, new Text("\tTotal Late Arrival Delay in minutes: " + totLateArrivalDelay));
        context.write(null, new Text("\tTotal Delay in minutes: " + totDelay));
        context.write(null, new Text("\tTotal Flights: " + totflights));
        context.write(null, new Text("\t% of Late Arrival Delay: " + ((float) totLateArrivalDelay / totDelay) * 100));
//        context.write(null, new Text("\tAvg Late Arrival Delay: " + (float) totLateArrivalDelay / totflights));

    }

    private class Delays {

        int lateArrivalDelay;
        int delay;
        int numFlights;

        public Delays(int lateArrivalDelay, int delay, int numFlights) {
            this.lateArrivalDelay = lateArrivalDelay;
            this.delay = delay;
            this.numFlights = numFlights;
        }

        public int getLateArrivalDelay() {
            return lateArrivalDelay;
        }

        public void setLateArrivalDelay(int lateArrivalDelay) {
            this.lateArrivalDelay = lateArrivalDelay;
        }

        public int getDelay() {
            return delay;
        }

        public void setDelay(int delay) {
            this.delay = delay;
        }

        public int getNumFlights() {
            return numFlights;
        }

        public void setNumFlights(int numFlights) {
            this.numFlights = numFlights;
        }

    }

}
