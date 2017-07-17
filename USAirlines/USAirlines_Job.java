/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package USAirlines;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import util.MapperOutput;

/**
 *
 * @author priyankb
 */
public class USAirlines_Job {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            // Give the MapRed job a name. You'll see this name in the Yarn webapp.
            Job job = Job.getInstance(conf, "USC-Priyank");

            // Current class.
            job.setJarByClass(USAirlines_Job.class);

            // Mapper
            job.setMapperClass(USAirlines_Mapper_1_2.class);
            // Combiner. We use the reducer as the combiner in this case.
            //job.setCombinerClass(USCensusCombiner.class);

            // Reducer
            job.setReducerClass(USAirlines_Reducer_1_2.class);
//            job.setPartitionerClass(USC_Partitioner.class);
//            job.setNumReduceTasks(USC_Partitioner.statesName.size());
            // Outputs from the Mapper.
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(MapperOutput.class);
//            job.setMapOutputValueClass(Text.class);

            // Outputs from Reducer. It is sufficient to set only the following
            // two properties
            // if the Mapper and Reducer has same key and value types. It is set
            // separately for
            // elaboration.
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            // path to input in HDFS
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileSystem fileSystem = FileSystem.get(conf);

            String outputPath = args[1] + "_temp";

            if (fileSystem.exists(new Path(outputPath))) {
                fileSystem.delete(new Path(outputPath), true);
            }
            // path to output in HDFS
            FileOutputFormat.setOutputPath(job, new Path(outputPath));
            // Block until the job is completed.

            Boolean isCompleted = job.waitForCompletion(true);
            if (isCompleted) {
////                Job job2 = Job.getInstance(conf, "USC-Priyank-2");
////                job2.setJarByClass(USC_Job.class);
////                // Mapper
////                job2.setMapperClass(USC_Mapper2.class);
////                // Reducer
////                job2.setReducerClass(USC_Reducer2.class);
////                job2.setNumReduceTasks(1);
////                // Outputs from the Mapper.
////                job2.setMapOutputKeyClass(Text.class);
////                job2.setMapOutputValueClass(Text.class);
////                // Outputs from Reducer. It is sufficient to set only the following
////                // two properties
////                // if the Mapper and Reducer has same key and value types. It is set
////                // separately for
////                // elaboration.
////                job2.setOutputKeyClass(Text.class);
////                job2.setOutputValueClass(Text.class);
////                // path to input in HDFS
////                FileInputFormat.addInputPath(job2, new Path(outputPath));
//////            FileSystem fileSystem = FileSystem.get(conf1);
////
////                String outputPath2 = args[1];
////
////                if (fileSystem.exists(new Path(outputPath2))) {
////                    fileSystem.delete(new Path(outputPath2), true);
////                }
////                // path to output in HDFS
////                FileOutputFormat.setOutputPath(job2, new Path(outputPath2));
//                System.exit(job2.waitForCompletion(true) ? 0 : 1);
                System.exit(0);
            }

//            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            System.err.println(e.getMessage());
        }

    }

}
