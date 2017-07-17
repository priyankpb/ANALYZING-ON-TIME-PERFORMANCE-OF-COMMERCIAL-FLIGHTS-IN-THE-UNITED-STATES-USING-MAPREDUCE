/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package USAirlines.Q6;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author priyankb
 */
public class Q6Combiner extends Reducer<Text, Q6MapperOutput, Text, Q6MapperOutput> {

    @Override
    protected void reduce(Text key, Iterable<Q6MapperOutput> values, Context context) throws IOException, InterruptedException {
        Map<Integer, List<Integer>> yearwiseValues = new HashMap<>();
        for (Q6MapperOutput next : values) {
            boolean planeData = next.getPlaneData().get();
            if (!planeData) {
                int year = next.getYear().get();
                List<Integer> stats = yearwiseValues.get(year);
                if (stats == null) {
                    stats = new ArrayList<>();
                    stats.add(0);
                    stats.add(0);
                    yearwiseValues.put(year, stats);
                } else {
                    stats.set(0, stats.get(0) + next.getDelay().get());
                    stats.set(1, stats.get(1) + 1);
                }

            } else {
                context.write(key, next);
            }

        }
        for (Map.Entry<Integer, List<Integer>> entrySet : yearwiseValues.entrySet()) {
            int year = entrySet.getKey();
            List<Integer> stats = entrySet.getValue();
            Q6MapperOutput q8MapperOutput = new Q6MapperOutput(new BooleanWritable(false), new IntWritable(year), new IntWritable(stats.get(0)), new IntWritable(stats.get(1)));
            context.write(key, q8MapperOutput);
        }
    }

}
