/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package USAirlines.Q12;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author priyankb
 */
public class Q12Reducer1 extends Reducer<Text, Q12MapperOutput, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Q12MapperOutput> mapperOutputs, Context context) throws IOException, InterruptedException {

        String keyString = key.toString();

        float totDelay = 0;
        float totRecords = 0;

        for (Q12MapperOutput mapperOutput : mapperOutputs) {
            int delay = mapperOutput.getDelay().get();
            int records = mapperOutput.getSum().get();

            totDelay += delay;
            totRecords += records;
        }

        float avgDelay = totDelay / totRecords;

        String tempResult = keyString + "#" + avgDelay;

        Text key2 = new Text();
        key2.set("KEY");

        Text result = new Text();
        result.set(tempResult);

        context.write(key2, result);
    }
}
