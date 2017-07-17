/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package USAirlines.Job;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author priyankb
 */
public class Airlines_Job {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            // Give the MapRed job a name. You'll see this name in the Yarn webapp.
            Job job = Job.getInstance(conf, "USAirlines-Priyank");

            // Current class.
            job.setJarByClass(Airlines_Job.class);

            // Mapper
            job.setMapperClass(USAirlines.Q6.Q6Mapper1.class);
            // Combiner. We use the reducer as the combiner in this case.
            job.setCombinerClass(USAirlines.Q6.Q6Combiner.class);

            // Reducer
            job.setReducerClass(USAirlines.Q6.Q6Reducer1.class);
//            job.setPartitionerClass(USC_Partitioner.class);
//            job.setNumReduceTasks(USC_Partitioner.statesName.size());
            // Outputs from the Mapper.
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(USAirlines.Q6.Q6MapperOutput.class);
//            job.setMapOutputValueClass(Text.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            // path to input in HDFS
            FileInputFormat.addInputPath(job, new Path(args[0]));
            //aux path
            if (args.length > 2) {
                FileInputFormat.addInputPath(job, new Path(args[2]));
            }

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
                Job job2 = Job.getInstance(conf, "USAirlines-Priyank-2");
                job2.setJarByClass(Airlines_Job.class);
                // Mapper
                job2.setMapperClass(USAirlines.Q6.Q6Mapper2.class);
                // Reducer
                job2.setReducerClass(USAirlines.Q6.Q6Reducer2.class);
                job2.setNumReduceTasks(1);
                // Outputs from the Mapper.
                job2.setMapOutputKeyClass(Text.class);
                job2.setMapOutputValueClass(Text.class);

                job2.setOutputKeyClass(Text.class);
                job2.setOutputValueClass(Text.class);
                // path to input in HDFS
                FileInputFormat.addInputPath(job2, new Path(outputPath));
//            FileSystem fileSystem = FileSystem.get(conf1);

                String outputPath2 = args[1];

                if (fileSystem.exists(new Path(outputPath2))) {
                    fileSystem.delete(new Path(outputPath2), true);
                }
                // path to output in HDFS
                FileOutputFormat.setOutputPath(job2, new Path(outputPath2));
                System.exit(job2.waitForCompletion(true) ? 0 : 1);

            }

//            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            System.err.println(e.getMessage());
        }

    }

}
