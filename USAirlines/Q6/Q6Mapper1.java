/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package USAirlines.Q6;

import java.io.IOException;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import util.Protocol;

/**
 *
 * @author priyankb
 */
public class Q6Mapper1 extends Mapper<LongWritable, Text, Text, Q6MapperOutput> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String row = value.toString();
        if (!row.startsWith("tailnum") && !row.startsWith("Year")) {
            String[] tokens = row.split(",");
            String first = tokens[0];
            if (first.charAt(0) == 'N') {
                try {
                    String year = tokens[Protocol.Airplanes.year];
                    if (!year.equalsIgnoreCase(Protocol.Airplanes.NONE)) {
                        int manYear = Integer.parseInt(year);
                        Q6MapperOutput mapperOutput = new Q6MapperOutput(new BooleanWritable(true), new IntWritable(manYear), new IntWritable(0), new IntWritable(0));
                        context.write(new Text(first), mapperOutput);
                    }

                } catch (Exception ex) {

                }
            } else {
                for (int i = 0; i < tokens.length; i++) {
                    if (tokens[i].equalsIgnoreCase("NA")) {
                        tokens[i] = "0";
                    }
                }
                int year = Integer.parseInt(tokens[Protocol.Year]);
                String tailNum = tokens[Protocol.TailNum];
                int arrDelay = Integer.parseInt(tokens[Protocol.ArrDelay]);
                int depDelay = Integer.parseInt(tokens[Protocol.DepDelay]);
                int delay = arrDelay + depDelay;
                Q6MapperOutput mapperOutput = new Q6MapperOutput(new BooleanWritable(false), new IntWritable(year), new IntWritable(delay), new IntWritable(1));
                context.write(new Text(tailNum), mapperOutput);
            }
        }

    }

}
