/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package USAirlines.Q12;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import util.Protocol;

/**
 *
 * @author priyankb
 */
public class Q12Mapper1 extends Mapper<LongWritable, Text, Text, Q12MapperOutput> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String row = value.toString();

        if (!row.startsWith("Year")) {

            String tokens[] = row.split(",");

            for (int i = 0; i < tokens.length; i++) {
                if (tokens[i].equalsIgnoreCase("NA")) {
                    tokens[i] = "0";
                }
            }

            int month = Integer.parseInt(tokens[Protocol.Month]);
            int dayofWeek = Integer.parseInt(tokens[Protocol.DayOfWeek]);
            int crsDepTime = Integer.parseInt(tokens[Protocol.CRSDepTime]);

            //delays
            int arrDelay = Integer.parseInt(tokens[Protocol.ArrDelay]);
            int depDelay = Integer.parseInt(tokens[Protocol.DepDelay]);
            int delay = arrDelay + depDelay;

            Q12MapperOutput mapperOutput = new Q12MapperOutput();
            mapperOutput.setDelay(new IntWritable(delay));
            mapperOutput.setSum(new IntWritable(1));

            context.write(new Text("TIME:" + Protocol.getHour(crsDepTime)), mapperOutput);
            context.write(new Text("DAY:" + dayofWeek), mapperOutput);
            context.write(new Text("MONTH:" + month), mapperOutput);
        }

    }
}
