/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package USAirlines.Q12;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import util.Protocol;

/**
 *
 * @author priyankb
 */
public class Q12Reducer2 extends Reducer<Text, Text, Text, Text> {

    Map<Integer, Float> timeMap = new HashMap();
    Map<Integer, Float> dayMap = new HashMap();
    Map<Integer, Float> monthMap = new HashMap();

    @Override
    protected void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {

        for (Text result : value) {

            String tempResult[] = result.toString().split("#");
            String tempCategory[] = tempResult[0].split(":");
            String category = tempCategory[0];
            int categoryValue = Integer.parseInt(tempCategory[1]);

            float totDelay = Float.parseFloat(tempResult[1]);

            switch (category) {
                case "TIME":
                    timeMap.put(categoryValue, totDelay);
                    break;

                case "DAY":
                    dayMap.put(categoryValue, totDelay);
                    break;

                case "MONTH":
                    monthMap.put(categoryValue, totDelay);
                    break;
            }
        }

        SortedSet<Map.Entry<Integer, Float>> sortedTimeSet = Protocol.sortMapByValue(timeMap);
        SortedSet<Map.Entry<Integer, Float>> sortedDaySet = Protocol.sortMapByValue(dayMap);
        SortedSet<Map.Entry<Integer, Float>> sortedMonthSet = Protocol.sortMapByValue(monthMap);

        //Q1
        Integer minTime = sortedTimeSet.first().getKey();
        Integer minDay = sortedDaySet.first().getKey();
        Integer minMonth = sortedMonthSet.first().getKey();

        context.write(new Text("---Q1---"), null);
        context.write(null, new Text("\n"));
        context.write(null, new Text("Best Time-of-the-day : " + Protocol.getTime(minTime)));
        context.write(null, new Text("Best Day-of-the-week : " + Protocol.getDay(minDay)));
        context.write(null, new Text("Best Time-of-the-year : " + Protocol.getMonth(minMonth)));
        context.write(null, new Text("\n\n\n"));

        //Q2
        Integer maxTime = sortedTimeSet.last().getKey();
        Integer maxDay = sortedDaySet.last().getKey();
        Integer maxMonth = sortedMonthSet.last().getKey();

        context.write(new Text("---Q2---"), null);
        context.write(null, new Text("\n"));
        context.write(null, new Text("Worst Time-of-the-day : " + Protocol.getTime(maxTime)));
        context.write(null, new Text("Worst Day-of-the-week : " + Protocol.getDay(maxDay)));
        context.write(null, new Text("Worst Time-of-the-year : " + Protocol.getMonth(maxMonth)));
        context.write(null, new Text("\n\n\n"));
    }
}
