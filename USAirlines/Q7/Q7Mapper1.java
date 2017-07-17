/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package USAirlines.Q7;

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
public class Q7Mapper1 extends Mapper<LongWritable, Text, Text, Q7MapperOutput> {

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

                int lateAircraftDelay = Integer.parseInt(tokens[Protocol.LateAircraftDelay]);
                String origin = tokens[Protocol.Origin];
                String dest = tokens[Protocol.Dest];

                int arrDelay = Integer.parseInt(tokens[Protocol.ArrDelay]);
                int depDelay = Integer.parseInt(tokens[Protocol.DepDelay]);
                int delay = arrDelay + depDelay;
                if (delay < 0) {
                    delay = 0;
                }

                Q7MapperOutput mapperOutputOrigin = new Q7MapperOutput();
                mapperOutputOrigin.setAirport(new Text(origin));
                mapperOutputOrigin.setAirportName(new Text("X"));
                mapperOutputOrigin.setLateArrivalDelay(new IntWritable(lateAircraftDelay));
                mapperOutputOrigin.setNumFlights(new IntWritable(1));
                mapperOutputOrigin.setDelay(new IntWritable(delay));

                context.write(new Text(origin), mapperOutputOrigin);

                Q7MapperOutput mapperOutputDest = new Q7MapperOutput();
                mapperOutputDest.setAirport(new Text(dest));
                mapperOutputDest.setAirportName(new Text("X"));
                mapperOutputDest.setLateArrivalDelay(new IntWritable(lateAircraftDelay));
                mapperOutputDest.setNumFlights(new IntWritable(1));
                mapperOutputDest.setDelay(new IntWritable(delay));

                context.write(new Text(dest), mapperOutputDest);

            }
        } else {
            if (!row.startsWith("\"iata\"")) {
                String[] tokens = row.split("\"");

                String airport = tokens[Protocol.Airports.iata];
                String airportName = tokens[Protocol.Airports.airport];

                Q7MapperOutput mapperOutput = new Q7MapperOutput();
                mapperOutput.setAirport(new Text(airport));
                mapperOutput.setAirportName(new Text(airportName));
                mapperOutput.setNumFlights(new IntWritable(-1));
                mapperOutput.setLateArrivalDelay(new IntWritable(-1));
                mapperOutput.setDelay(new IntWritable(-1));

                context.write(new Text(airport), mapperOutput);

            }
        }
    }
}
