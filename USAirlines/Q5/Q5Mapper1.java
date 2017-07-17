/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package USAirlines.Q5;

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
public class Q5Mapper1 extends Mapper<LongWritable, Text, Text, Q5MapperOutput> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String row = value.toString();
        if (!row.contains("\"")) {
            if (!row.startsWith("Year") && !row.startsWith("Code")) {
                String tokens[] = row.split(",");

                for (int i = 0; i < tokens.length; i++) {
                    if (tokens[i].equalsIgnoreCase("NA")) {
                        tokens[i] = "0";
                    }
                }
                String carrier = tokens[Protocol.UniqueCarrier];
                int arrDelay = Integer.parseInt(tokens[Protocol.ArrDelay]);
                int depDelay = Integer.parseInt(tokens[Protocol.DepDelay]);
                int delay = arrDelay + depDelay;

                Q5MapperOutput mapperOutput = new Q5MapperOutput();
                mapperOutput.setCarrier(new Text(carrier));
                mapperOutput.setCarrierName(new Text("X"));
                mapperOutput.setDelay(new IntWritable(delay));
                mapperOutput.setNumFlights(new IntWritable(1));

                context.write(new Text(carrier), mapperOutput);
            }
        } else {
            if (!row.startsWith("Code")) {
                String[] tokens = row.split("\"");

                String carrier = tokens[Protocol.Carriers.code];
                String carrierName = tokens[Protocol.Carriers.description];
                
                Q5MapperOutput mapperOutput = new Q5MapperOutput();
                mapperOutput.setCarrier(new Text(carrier));
                mapperOutput.setCarrierName(new Text(carrierName));
                mapperOutput.setDelay(new IntWritable(-1));
                mapperOutput.setNumFlights(new IntWritable(-1));
                
                context.write(new Text(carrier), mapperOutput);
            }
        }
    }
}
