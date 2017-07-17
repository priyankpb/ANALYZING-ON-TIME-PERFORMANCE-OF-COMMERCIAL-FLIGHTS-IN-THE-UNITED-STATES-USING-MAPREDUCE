/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package USAirlines.Q4;

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
public class Q4Mapper1 extends Mapper<LongWritable, Text, Text, Q4MapperOutput> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String row = value.toString();
        if (!row.contains("\"")) {
            if (!row.startsWith("Year")) {

                String tokens[] = row.split(",");

                for (int i = 0; i < tokens.length; i++) {
                    if (tokens[i].equalsIgnoreCase("NA")) {
                        tokens[i] = "0";
                    }
                }

                String origin = tokens[Protocol.Origin];
                String dest = tokens[Protocol.Dest];
                int weatherDelay = Integer.parseInt(tokens[Protocol.WeatherDelay]);

                Q4MapperOutput mapperOutputOrigin = new Q4MapperOutput();
                mapperOutputOrigin.setAirport(new Text(origin));
                mapperOutputOrigin.setCity(new Text("X"));
                mapperOutputOrigin.setWeatherDelay(new IntWritable(weatherDelay));
                mapperOutputOrigin.setRecords(new IntWritable(1));

                context.write(new Text(origin), mapperOutputOrigin);

                Q4MapperOutput mapperOutputDest = new Q4MapperOutput();
                mapperOutputDest.setAirport(new Text(dest));
                mapperOutputDest.setCity(new Text("X"));
                mapperOutputDest.setWeatherDelay(new IntWritable(weatherDelay));
                mapperOutputDest.setRecords(new IntWritable(1));

                context.write(new Text(dest), mapperOutputDest);

            }
        } else {
            if (!row.startsWith("\"iata\"")) {
                String[] tokens = row.split("\"");

                String airport = tokens[Protocol.Airports.iata];
                String city = tokens[Protocol.Airports.city];

                Q4MapperOutput mapperOutput = new Q4MapperOutput();
                mapperOutput.setAirport(new Text(airport));
                mapperOutput.setCity(new Text(city));
                mapperOutput.setRecords(new IntWritable(-1));
                mapperOutput.setWeatherDelay(new IntWritable(-1));

                context.write(new Text(airport), mapperOutput);

            }
        }
    }
}
