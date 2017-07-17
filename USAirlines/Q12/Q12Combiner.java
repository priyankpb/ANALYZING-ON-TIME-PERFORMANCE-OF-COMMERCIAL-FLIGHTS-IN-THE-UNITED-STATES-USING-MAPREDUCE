/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package USAirlines.Q12;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author priyankb
 */
public class Q12Combiner extends Reducer<Text, Q12MapperOutput, Text, Q12MapperOutput> {

    @Override
    protected void reduce(Text key, Iterable<Q12MapperOutput> mapperOutputs, Context context) throws IOException, InterruptedException {

        String keyString = key.toString();

        int totDelay = 0;
        int totRecords = 0;

        for (Q12MapperOutput next : mapperOutputs) {
            int delay = next.getDelay().get();
            int Records = next.getSum().get();

            totDelay += delay;
            totRecords += Records;
        }

        Q12MapperOutput mapperOutput = new Q12MapperOutput();
        mapperOutput.setDelay(new IntWritable(totDelay));
        mapperOutput.setSum(new IntWritable(totRecords));

        context.write(key, mapperOutput);
    }
}
