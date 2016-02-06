/**
 * Samoylov Denis: pdc_shad 2015                                                         
 *                                                                                       
 * DistributionFollowers.java: Calculation of the distribution of the number followers   
 *                                                                                       
 * Input Data:  <user_id>[space]<follower_id>                                            
 * Output Data: <number of followers>[Tab]<frequency>                                    
*/

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * StageOne - Calculation of the number of followers
 * StageTwo - Calculation of the distribution of the number followers 
*/
public class DistributionFollowers {

    /**
     * Input:  key - user_id; value - follower_id
     * Output: key - user_id; value - const(1)
    */ 
    public static class MapperStageOne
            extends Mapper<Text, Text, IntWritable, IntWritable> {

        private final static IntWritable ONE_CONST = new IntWritable(1);
        private IntWritable mUserId = new IntWritable();
      
        public void map(Text key, Text value, Context context) 
                throws IOException, InterruptedException {

            mUserId.set(Integer.parseInt(key.toString()));
            context.write(mUserId, ONE_CONST);
        }
    }
    
    /**
     * Input:  key - user_id; value - list of the number of followers 
     * Output: key - user_id; value - total number of followers the user
    */
    public static class ReducerStageOne 
            extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        
        private IntWritable mNumberFollowers = new IntWritable();

        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) 
                throws IOException, InterruptedException {
     
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            mNumberFollowers.set(sum);
            context.write(key, mNumberFollowers);
        }
    }

    /** 
     * Input:  key - user_id; value - number of followers
     * Output: key - number of followers; value - const(1)
    */
    public static class MapperStageTwo
            extends Mapper<Text, Text, IntWritable, IntWritable> {

        private final static IntWritable ONE_CONST = new IntWritable(1);
        private IntWritable mNumberFollowers = new IntWritable();
      
        public void map(Text key, Text value, Context context) 
                throws IOException, InterruptedException {

            mNumberFollowers.set(Integer.parseInt(value.toString()));
            context.write(mNumberFollowers, ONE_CONST);
        }
    }
    
    
    /**
     * Input:  key - number of followers; value - list with a frequency of the number of followers
     * Output: key - number of followers; value - total frequency 
    */ 
    public static class ReducerStageTwo
            extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        
        private IntWritable mFrequency = new IntWritable();

        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) 
                throws IOException, InterruptedException {
     
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            mFrequency.set(sum);
            context.write(key, mFrequency);
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration confStageOne = new Configuration();
        Configuration confStageTwo = new Configuration();

        String[] otherArgs = new GenericOptionsParser(confStageOne, args).getRemainingArgs();

        if (otherArgs.length != 2) {
            System.err.println("Usage: top50 <in> <out>");
            System.exit(2);
        }
    
        // Delimiter read - space, for the first job
        confStageOne.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", " ");
        
        // Create the two jobs
        Job jobStageOne = new Job(confStageOne, "distrubutionfollowers");
        Job jobStageTwo = new Job(confStageTwo, "distrubutionfollowers");
       
        // Set number of reduce tasks for the second job
        jobStageTwo.setNumReduceTasks(1); 
       
        // Set class 
        jobStageOne.setJarByClass(DistributionFollowers.class);
        jobStageTwo.setJarByClass(DistributionFollowers.class);

        // Set mapper and reducer: first job
        jobStageOne.setMapperClass(MapperStageOne.class);
        jobStageOne.setCombinerClass(ReducerStageOne.class);
        jobStageOne.setReducerClass(ReducerStageOne.class);

        // Set mapper and reducer: second job
        jobStageTwo.setMapperClass(MapperStageTwo.class);
        jobStageTwo.setCombinerClass(ReducerStageTwo.class);
        jobStageTwo.setReducerClass(ReducerStageTwo.class);
    
        // Input/Output
        jobStageOne.setInputFormatClass(KeyValueTextInputFormat.class);
        jobStageOne.setOutputFormatClass(TextOutputFormat.class);
    
        jobStageTwo.setInputFormatClass(KeyValueTextInputFormat.class);
        jobStageTwo.setOutputFormatClass(TextOutputFormat.class);
        
        jobStageOne.setOutputKeyClass(IntWritable.class);
        jobStageOne.setOutputValueClass(IntWritable.class);
    
        jobStageTwo.setOutputKeyClass(IntWritable.class);
        jobStageTwo.setOutputValueClass(IntWritable.class);
    
        // Create a temporary directory for the intermediate result
        FileInputFormat.addInputPath(jobStageOne, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(jobStageOne, new Path("./tmp_distfollowers"));
    
        FileInputFormat.addInputPath(jobStageTwo, new Path("./tmp_distfollowers"));
        FileOutputFormat.setOutputPath(jobStageTwo, new Path(otherArgs[1]));
    
        if (jobStageOne.waitForCompletion(true)) {
            boolean status = jobStageTwo.waitForCompletion(true);
            // Delete the temporary directory
            FileSystem fs = FileSystem.get(new Configuration());
          //  fs.delete(new Path("./tmp_distfollowers"), true);
      
            if (status) {
                System.exit(0);
            } else {
                System.exit(1);
            }
        } else {
            System.exit(1);
        }
    }
}
