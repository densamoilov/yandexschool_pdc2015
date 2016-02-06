/**
 * Samoylov Denis: pdc_shad 2015                                               
 *                                                                             
 * AverageFollowers.java: The calculation of the average number of followers   
 *                                                                             
 * Input Data:  <user_id>[space]<follower_id>                                  
 * Output Data: <average number of followers>                                  
*/

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
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
 * StageTwo - The calculation of the average number of followers
*/
public class AverageFollowers {
 
    public static class MyPair implements Writable {
        
        private IntWritable mFirst;
        private IntWritable mSecond;

        public MyPair() {
            set(new IntWritable(), new IntWritable());
        }

        public MyPair(int first, int second) {
            set(new IntWritable(first), new IntWritable(second));
        }

        public void set(IntWritable first, IntWritable second) {
            this.mFirst = first;
            this.mSecond = second;
        }

        public IntWritable getFirst() {
            return mFirst;
        }

        public IntWritable getSecond() {
            return mSecond;
        }  
    
        @Override
        public void write(DataOutput out) throws IOException {
            mFirst.write(out);
            mSecond.write(out);
        } 

        @Override
        public void readFields(DataInput in) throws IOException {
            mFirst.readFields(in);
            mSecond.readFields(in);
        }
    }

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
     * Output: key - const(1); value - pair(number of followers, const(1))
    */  
    public static class MapperStageTwo 
            extends Mapper<Text, Text, IntWritable, MyPair> {

        private final static IntWritable ONE_CONST = new IntWritable(1);
        private IntWritable mNumberFollowers = new IntWritable();
        private MyPair mAvgPair = new MyPair();
      
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {

            mNumberFollowers.set(Integer.parseInt(value.toString()));
            mAvgPair.set(mNumberFollowers, ONE_CONST);

            context.write(ONE_CONST, mAvgPair);
        }
    }
  
    /**
     * Input:  key - const(1); value - list of the pair(number of followers, count users)
     * Output: key - const(1); value - pair(number of followers, count users)
    */
    public static class CombinerStageTwo 
            extends Reducer<IntWritable, MyPair, IntWritable, MyPair> {
    
        private final static IntWritable ONE_CONST = new IntWritable(1);
        private IntWritable mNumberFollowers = new IntWritable();
        private IntWritable mCountUsers = new IntWritable();
        private MyPair mAvgPair = new MyPair();

        public void reduce(IntWritable key, Iterable<MyPair> values, Context context) 
                throws IOException, InterruptedException {
            
            int sum = 0;
            int cnt = 0;

            for (MyPair val : values) {
                sum += val.getFirst().get();
                cnt += val.getSecond().get();
            }

            mNumberFollowers.set(sum);
            mCountUsers.set(cnt);
            mAvgPair.set(mNumberFollowers, mCountUsers);
            
            context.write(ONE_CONST, mAvgPair);
        }
    }
    
    /**
     * Input:  key - const(1); value - list of the pair(number of followers, count users)
     * Output: key - "Average followers: "; value - average number of followers
    */
    public static class ReducerStageTwo
            extends Reducer<IntWritable, MyPair, Text, DoubleWritable> {

        public void reduce(IntWritable key, Iterable<MyPair> values, Context context) 
                throws IOException, InterruptedException {
      
            int sum = 0;
            int cnt = 0;

            for (MyPair val : values) {
                sum += val.getFirst().get();
                cnt += val.getSecond().get();
            }
            double avg = (double)sum / (double)cnt;

            context.write(new Text("Average followers: "), new DoubleWritable(avg));
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
        Job jobStageOne = new Job(confStageOne, "averagefollowers");
        Job jobStageTwo = new Job(confStageTwo, "averagefollowers");
        
        // Set number of reduce tasks for the second job
        jobStageTwo.setNumReduceTasks(1);
        
        // Set class 
        jobStageOne.setJarByClass(AverageFollowers.class);
        jobStageTwo.setJarByClass(AverageFollowers.class);

        // Set mapper and reducer: first job
        jobStageOne.setMapperClass(MapperStageOne.class);
        jobStageOne.setCombinerClass(ReducerStageOne.class);
        jobStageOne.setReducerClass(ReducerStageOne.class);

        // Set mapper and reducer: second job
        jobStageTwo.setMapperClass(MapperStageTwo.class);
        jobStageTwo.setCombinerClass(CombinerStageTwo.class);
        jobStageTwo.setReducerClass(ReducerStageTwo.class);
    
        // Input/Output
        jobStageOne.setInputFormatClass(KeyValueTextInputFormat.class);
        jobStageOne.setOutputFormatClass(TextOutputFormat.class);
    
        jobStageTwo.setInputFormatClass(KeyValueTextInputFormat.class);
        jobStageTwo.setOutputFormatClass(TextOutputFormat.class);
        
        jobStageOne.setOutputKeyClass(IntWritable.class);
        jobStageOne.setOutputValueClass(IntWritable.class);
    
        jobStageTwo.setMapOutputKeyClass(IntWritable.class);
        jobStageTwo.setMapOutputValueClass(MyPair.class);
    
        // Create a temporary directory for the intermediate result
        FileInputFormat.addInputPath(jobStageOne, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(jobStageOne, new Path("./tmp_avg"));
    
        FileInputFormat.addInputPath(jobStageTwo, new Path("./tmp_avg"));
        FileOutputFormat.setOutputPath(jobStageTwo, new Path(otherArgs[1]));
    
        if (jobStageOne.waitForCompletion(true)) {
            boolean status = jobStageTwo.waitForCompletion(true);
            // Delete the temporary directory
            FileSystem fs = FileSystem.get(new Configuration());
            fs.delete(new Path("./tmp_avg"), true);
      
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