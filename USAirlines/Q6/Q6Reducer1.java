/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package USAirlines.Q6;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author priyankb
 */
public class Q6Reducer1 extends Reducer<Text, Q6MapperOutput, IntWritable, Text> {

    @Override
    protected void reduce(Text key, Iterable<Q6MapperOutput> values, Context context) throws IOException, InterruptedException {
        boolean found = false;
        Integer manuYear = null;
        List<Q6MapperOutput> data = new ArrayList<>();
        for (Q6MapperOutput value : values) {
            boolean planeData = value.getPlaneData().get();
            if (!planeData) {
                Q6MapperOutput obj = new Q6MapperOutput(new BooleanWritable(false), new IntWritable(value.getYear().get()), new IntWritable(value.getDelay().get()), new IntWritable(value.getNumFlights().get()));
                data.add(obj);
            } else {
                found = true;
                manuYear = Integer.parseInt(value.getYear().toString());
            }
        }
        if (found) {
            for (Q6MapperOutput data1 : data) {
                int year = data1.getYear().get();
                int delay = data1.getDelay().get();
                int numFlights = data1.getNumFlights().get();
                int age = year - manuYear;
                if (year >= manuYear && age <= 100) {
                    String result = delay + "#" + numFlights;
                    context.write(new IntWritable(age), new Text(result));
                }
            }
        }

    }

}
