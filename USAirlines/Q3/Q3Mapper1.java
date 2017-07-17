/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package USAirlines.Q3;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import util.Protocol;
import USAirlines.Q12.Q12MapperOutput;

/**
 *
 * @author priyankb
 */
public class Q3Mapper1 extends Mapper<LongWritable, Text, Text, Q12MapperOutput> {

    @Override
    protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {

        String row = value.toString();
        if (!row.contains("\"")) {
            if (!row.startsWith("Year")) {
                String tokens[] = row.split(",");

                for (int i = 0; i < tokens.length; i++) {
                    if (tokens[i].equalsIgnoreCase("NA")) {
                        tokens[i] = "0";
                    }
                }

                int year = Integer.parseInt(tokens[Protocol.Year]);
                String origin = tokens[Protocol.Origin];
                String dest = tokens[Protocol.Dest];
                int numFlights = 1;

                Q3MapperOutput mapperOutputOrigin = new Q3MapperOutput();
                mapperOutputOrigin.setYear(new IntWritable(year));
                mapperOutputOrigin.setAirport(new Text(origin));
                mapperOutputOrigin.setAirportName(new Text("X"));
                mapperOutputOrigin.setNumFlights(new IntWritable(numFlights));

                context.write(new Text(Integer.toString(year)), mapperOutputOrigin);

                Q3MapperOutput mapperOutputDest = new Q3MapperOutput();
                mapperOutputDest.setYear(new IntWritable(year));
                mapperOutputDest.setAirport(new Text(dest));
                mapperOutputDest.setAirportName(new Text("X"));
                mapperOutputDest.setNumFlights(new IntWritable(numFlights));

                context.write(new Text(Integer.toString(year)), mapperOutputDest);
            }
        } else {
            if (!row.startsWith("\"iata\"")) {
                String[] tokens = row.split("\"");

                String airport = tokens[Protocol.Airports.iata];
                String airportName = tokens[Protocol.Airports.airport];

                Q3MapperOutput mapperOutput = new Q3MapperOutput();
                mapperOutput.setAirport(new Text(airport));
                mapperOutput.setAirportName(new Text(airportName));
                mapperOutput.setNumFlights(new IntWritable(-1));
                mapperOutput.setYear(new IntWritable(-1));

                context.write(new Text(airport), mapperOutput);

            }
        }
    }
}
