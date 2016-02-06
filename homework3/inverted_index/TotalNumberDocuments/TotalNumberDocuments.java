/**
 * Samoylov Denis: pdc_shad 2015                                               
 *                                                                             
 * TotalNumberDocuments.java: Calculation of the number of documents (articles)
 *                                                                             
 * Input Data:  *.xml file (dump of wikiedia)                                  
 * Output Data: <number of documents>
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



public class TotalNumberDocuments {
    /**
     * Input:  key - docid; value - content
     * Output: key - const(1); value - const(1)
    */
    public static class MapperSumDocuments
            extends Mapper<Text, Text, IntWritable, IntWritable> {
    
        private IntWritable ONE_COST = new IntWritable(1);
        
        public void map(Text key, Text value, Context context) 
                throws IOException, InterruptedException {

                context.write(ONE_COST, ONE_COST);
        }         
    }
  
    /**
     * Input:  kkey - const(1); value - list(1)
     * Output: key - const(1); value - local number of documents
    */
    public static class CombinerSumDocuments
            extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

        private IntWritable mLocalNumberDocuments = new IntWritable();
        private IntWritable ONE_CONST = new IntWritable(1);

        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();                
            }
            mLocalNumberDocuments.set(sum);

            context.write(ONE_CONST, mLocalNumberDocuments);
        }
    }
    
    /**
     * Input:  kkey - const(1); value - list(local number of documents)
     * Output: <total naumber of documents>
    */
    public static class ReducerSumDocuments
            extends Reducer<IntWritable, IntWritable, Text, IntWritable> {

        private IntWritable mTotalNumberDocuments = new IntWritable();

        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();                
            }
            mTotalNumberDocuments.set(sum);

            context.write(new Text("Total number of documents: "), mTotalNumberDocuments);
        }
    }

   
    public static void main(String[] args) throws Exception {
    
        Configuration conf = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    
        if (otherArgs.length != 2) {
            System.err.println("Usage: wordcount <in> <out>");
            System.exit(2);
        }

        conf.setBoolean("exact.match.only", true);
        conf.set("io.serializations",
             "org.apache.hadoop.io.serializer.JavaSerialization,"
             + "org.apache.hadoop.io.serializer.WritableSerialization");


        Job job = new Job(conf, "totalnumberdocuments");

        // Set class 
        job.setJarByClass(TotalNumberDocuments.class);

        job.setMapperClass(MapperSumDocuments.class);
        job.setCombinerClass(CombinerSumDocuments.class);
        job.setReducerClass(ReducerSumDocuments.class);

        // Input/Output
        job.setInputFormatClass(XmlInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
