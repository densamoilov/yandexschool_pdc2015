/**
 * Samoylov Denis: pdc_shad 2015                                               
 *                                                                             
 * HighFrequencyTop.java: The determined top high-frequency words
 *                                                                             
 * Input Data:  *.xml file (dump of wikiedia)                                  
 * Output Data: "String[] mHighFrequencyTop = {word1, word2,...,wordn};"
*/

package shad.homework3.task2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.TreeSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * StageOne - Calculation of the total term frequency for the every words
 * StageTwo - Determination the top 20 words by the term frequency
*/
public class HighFrequencyTop {

    /**
     * Input:  key - docid; value - content
     * Output: key - word; value - term frequency
    */
    public static class MapperStageOne
            extends Mapper<Text, Text, Text, IntWritable> {
    
        private IntWritable mTermFrequency = new IntWritable();
        private Text mTerm = new Text();
         
        private TreeSet<String> mProcessedTerms = new TreeSet();
        
        public void map(Text key, Text value, Context context) 
                throws IOException, InterruptedException {
                        
            StringTokenizer itr = new StringTokenizer(value.toString());
            
            mProcessedTerms.clear();
            
            while (itr.hasMoreTokens()) {
                String term = itr.nextToken().replaceAll("\\p{Punct}", "");
                if (term.length() < 4 || mProcessedTerms.contains(term)) {
                    continue;
                }
                mProcessedTerms.add(term);
                
                int tf = (value.toString() + "\0").split(term).length - 1;
                if (tf == 0) {
                    tf = 1;
                }
                
                mTerm.set(term);
                mTermFrequency.set(tf);                
    
                context.write(mTerm, mTermFrequency);
            }         
        }
    }
    
    /**
     * Input:  key - word; value - term frequency
     * Output: key - word; value - total term frequency
    */
    public static class ReducerStageOne 
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable mTotalTermFrequency = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();                
            }
            mTotalTermFrequency.set(sum);
            context.write(key, mTotalTermFrequency);
        }
    }

   /**
     * Input:  key - word; value - term frequency
     * Output: key - term frequency; value - word
    */
    public static class MapperStageTwo 
            extends Mapper<Text, Text, IntWritable, Text> {
        
        private IntWritable mTermFrequency = new IntWritable();
          
        public void map(Text key, Text value, Context context) 
                throws IOException, InterruptedException {
            
            // With a minus - for sort by descending
            mTermFrequency.set(-Integer.parseInt(value.toString()));
      
            context.write(mTermFrequency, key);
        }
    }
    
   /**
     * Input:  key - term frequency; value - word
     * Output: (local top21 by term frequency) key - term frequency; value - word
    */  
    public static class CombinerStageTwo
            extends Reducer<IntWritable, Text, IntWritable, Text> {
        
        private int mStop = 0;    
       
        public void reduce(IntWritable key, Iterable<Text> values, Context context) 
                throws IOException, InterruptedException {

            for (Text val : values) {
                if (mStop < 21) {
                    context.write(key, val);
                    ++mStop;
                } else {
                    break;
                }
            }
        }
    }
     
     
   /**
     * Input:  key - term frequency; value - word
     * Output: (Total top20 by term frequency) "String[] mHighFrequencyTop = {word1, word2,...,wordn};
    */  
    public static class ReducerStageTwo
            extends Reducer<IntWritable, Text, Text, IntWritable> {
        
        private boolean mIsWrited = false;
        private int mStop = 0;    
        private StringBuilder mResult = new StringBuilder("String[] mHighFrequencyTop = ");
        
        public void reduce(IntWritable key, Iterable<Text> values, Context context) 
                throws IOException, InterruptedException {
 
        
            for (Text val : values) {
                if (mStop < 20) {
                    
                    if (mStop == 0) {
                        mResult.append("{" + "\"" + val.toString() + "\"" + ",");
                    } else if (mStop < 19) {
                        mResult.append("\"" + val.toString() + "\"" + ",");
                    } else {
                        mResult.append("\"" + val.toString() + "\"" + "};");
                    }
                    ++mStop; 
                } else {
                    if (!mIsWrited) {
                        context.write(new Text(mResult.toString()), new IntWritable(mStop));
                        mIsWrited = true;               
                    }
                    break;
                }
            }  
        }
    }


    public static void main(String[] args) throws Exception {
    
        Configuration confStageOne = new Configuration();
        Configuration confStageTwo = new Configuration();

        String[] otherArgs = new GenericOptionsParser(confStageOne, args).getRemainingArgs();
    
        if (otherArgs.length != 2) {
            System.err.println("Usage: wordcount <in> <out>");
            System.exit(2);
        }

        confStageOne.setBoolean("exact.match.only", true);
        confStageOne.set("io.serializations",
             "org.apache.hadoop.io.serializer.JavaSerialization,"
             + "org.apache.hadoop.io.serializer.WritableSerialization");

       
        // Create the two jobs
        Job jobStageOne = new Job(confStageOne, "highfrequencytop");
        Job jobStageTwo = new Job(confStageTwo, "highfrequencytop");
        
        // Set number of reduce tasks for the second job
        jobStageTwo.setNumReduceTasks(1);

        // Set class 
        jobStageOne.setJarByClass(HighFrequencyTop.class);
        jobStageTwo.setJarByClass(HighFrequencyTop.class);

        // Set mapper and reducer: first job
        jobStageOne.setMapperClass(MapperStageOne.class);
        jobStageOne.setCombinerClass(ReducerStageOne.class);
        jobStageOne.setReducerClass(ReducerStageOne.class);

        // Set mapper and reducer: second job
        jobStageTwo.setMapperClass(MapperStageTwo.class);
        jobStageTwo.setCombinerClass(CombinerStageTwo.class);
        jobStageTwo.setReducerClass(ReducerStageTwo.class);

        // Input/Output
        jobStageOne.setInputFormatClass(XmlInputFormat.class);
        jobStageOne.setOutputFormatClass(TextOutputFormat.class);
    
        jobStageTwo.setInputFormatClass(KeyValueTextInputFormat.class);
        jobStageTwo.setOutputFormatClass(TextOutputFormat.class);
        
        jobStageOne.setOutputKeyClass(Text.class);
        jobStageOne.setOutputValueClass(IntWritable.class);
    
        jobStageTwo.setMapOutputKeyClass(IntWritable.class);
        jobStageTwo.setMapOutputValueClass(Text.class);
    
        // Create a temporary directory for the intermediate result
        FileInputFormat.addInputPath(jobStageOne, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(jobStageOne, new Path("./tmp_hftop20"));
    
        FileInputFormat.addInputPath(jobStageTwo, new Path("./tmp_hftop20"));
        FileOutputFormat.setOutputPath(jobStageTwo, new Path(otherArgs[1]));

        if (jobStageOne.waitForCompletion(true)) {
            boolean status = jobStageTwo.waitForCompletion(true);
            // Delete the temporary directory
            FileSystem fs = FileSystem.get(new Configuration());
            fs.delete(new Path("./tmp_hftop20"), true);
      
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