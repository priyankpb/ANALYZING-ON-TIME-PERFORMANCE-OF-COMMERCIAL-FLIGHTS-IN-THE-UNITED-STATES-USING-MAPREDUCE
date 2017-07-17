/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package USAirlines.Q4;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author priyankb
 */
public class Q4Combiner extends Reducer<Text, Q4MapperOutput, Text, Q4MapperOutput> {

    @Override
    protected void reduce(Text key, Iterable<Q4MapperOutput> mapperOutputs, Context context) throws IOException, InterruptedException {

        String keyString = key.toString();

        int totDelay = 0;
        int totRecords = 0;

        for (Q4MapperOutput next : mapperOutputs) {
            int delay = next.getWeatherDelay().get();
            int records = next.getRecords().get();

            if (delay == -1 && records == -1) {
                context.write(key, next);
            } else {
                totDelay += delay;
                totRecords += records;
            }
        }

        Q4MapperOutput mapperOutput = new Q4MapperOutput();
        mapperOutput.setCity(new Text("X"));
        mapperOutput.setAirport(key);
        mapperOutput.setRecords(new IntWritable(totRecords));
        mapperOutput.setWeatherDelay(new IntWritable(totDelay));

        context.write(key, mapperOutput);

    }

}
