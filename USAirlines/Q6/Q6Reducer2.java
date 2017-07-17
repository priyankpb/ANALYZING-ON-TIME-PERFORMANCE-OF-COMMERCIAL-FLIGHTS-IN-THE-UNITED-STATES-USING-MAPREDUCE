/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package USAirlines.Q6;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author priyankb
 */
public class Q6Reducer2 extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Map<Integer, Data> dataStructure = new TreeMap<>();
        int totSum = 0;
        int totCount = 0;
        for (Text next : values) {
            String valueString = next.toString();
            String[] splitData = valueString.split("\t");
            int age = Integer.parseInt(splitData[0]);
            String statsStr = splitData[1];
            String[] splitStats = statsStr.split("#");
            int sum = Integer.parseInt(splitStats[0]);
            int count = Integer.parseInt(splitStats[1]);
            Data stats = dataStructure.get(age);
            if (stats == null) {
                stats = new Data(0, 0);
                dataStructure.put(age, stats);
            }
            stats.setDelay(stats.getDelay() + sum);
            stats.setNumFlights(stats.getNumFlights() + count);
            totCount += count;
            totSum += sum;
        }
        context.write(null, new Text(String.format("%-10s%-15s%-15s%-15s%-15s%-15s%-25s", "Age", "Delay", "% Delay", "numFlights", "% NumFlights", "Avg. Delay", "% Delay / % NumFlights")));
        for (Map.Entry<Integer, Data> entrySet : dataStructure.entrySet()) {
            Integer age = entrySet.getKey();
            Data stats = entrySet.getValue();
            stats.setPercentNumFlights(stats.getNumFlights() * 100.0f / totCount);
            stats.setPercentDelay(stats.getDelay() * 100.0f / totSum);
            context.write(null, new Text(String.format("%-10s%-15s%-15s%-15s%-15s%-15s%-25s", age, stats.getDelay(), stats.getPercentDelay() + "%", stats.getNumFlights(), stats.getPercentNumFlights() + "%", stats.getDelay() * 1.0f / stats.getNumFlights(), stats.getPercentDelay() / stats.getPercentNumFlights())));
        }

    }

    public class Data {

        int delay;
        int numFlights;
        float percentDelay;
        float percentNumFlights;

        public Data(int sum, int count) {
            this.delay = sum;
            this.numFlights = count;
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

        public float getPercentDelay() {
            return percentDelay;
        }

        public void setPercentDelay(float percentDelay) {
            this.percentDelay = percentDelay;
        }

        public float getPercentNumFlights() {
            return percentNumFlights;
        }

        public void setPercentNumFlights(float percentNumFlights) {
            this.percentNumFlights = percentNumFlights;
        }

    }

}
