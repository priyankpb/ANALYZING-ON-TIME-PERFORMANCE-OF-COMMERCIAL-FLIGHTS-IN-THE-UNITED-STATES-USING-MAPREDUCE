/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package USAirlines.Q4;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author priyankb
 */
public class Q4Reducer1 extends Reducer<Text, Q4MapperOutput, Text, Text> {

    Map<String, Integer> data = new HashMap();

    @Override
    protected void reduce(Text key, Iterable<Q4MapperOutput> mapperOutputs, Context context) throws IOException, InterruptedException {

        String city = null;
        String airport = key.toString();
        float totWeatherDelay = 0;
        int totRecords = 0;

        for (Q4MapperOutput mapperOutput : mapperOutputs) {
            String airport2 = mapperOutput.getAirport().toString();
            int delay = mapperOutput.getWeatherDelay().get();
            int records = mapperOutput.getRecords().get();

            if (delay == -1 && records == -1) {
                city = mapperOutput.getCity().toString();
            } else {
                totWeatherDelay += delay;
                totRecords += records;
            }

        }

//        float avgDelay = totWeatherDelay / totRecords;
        String tempResult = city + "#" + airport + "#" + totWeatherDelay + "#" + totRecords;

        Text key2 = new Text();
        key2.set("KEY");

        Text result = new Text();
        result.set(tempResult);

        context.write(key2, result);
    }

}
