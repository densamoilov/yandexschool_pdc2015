/**
 * Samoylov Denis: pdc_shad 2015                                               
 *                                                                             
 * InvertedIndex.java: The construction of the inverted index
 *                                                                             
 * Input Data:  *.xml file (dump of wikiedia)                                  
 * Output Data: (word, [<docid1, TF-IDF1>, <docid2, TF-IDF2>, ..., <docidn, TF-IDFn> ])
*/

package shad.homework3.task2;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.Math;
import java.lang.StringBuilder;
import java.util.StringTokenizer;
import java.util.TreeSet;
import java.util.Vector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;


public class InvertedIndex {    
    
    /**
     * It contains the top20 high-frequency words of the document 
     * Is used to exclude them from the index
     * Top high-frequency words is determined by a separate MapReduce program
    */
    public static class HighFrequencyTop {
        private TreeSet<String> mTop = new TreeSet<String>();
        
        public HighFrequencyTop() {
        
            String[] mHighFrequencyTop20 = {"Викиверситет","проект","Викиверситета","Ivan","Shmakov",
                                            "страниц","может","слов","участник","которые","можно","Викиверсите",
                                            "быть","праав","материал","wiki","2014","язык","Участник","User"};
            
            for (int i = 0; i < 20; ++i) {
                mTop.add(mHighFrequencyTop20[i]);
            }
        }
        
        public boolean contains(String str) {
            return mTop.contains(str);
        }            
    }                

    /**
     * Pair(term frequency, docid)
     * For a vector, in which are stored the 20 most relevant articles for a particular word
    */
    public static class PairTfDocId {
        private int mFirst;
        private int mSecond;

        public PairTfDocId() {}

        public PairTfDocId(int first, int second) {
            this.mFirst = first;
            this.mSecond = second;
        }
        public void set(int first, int second) {
            this.mFirst = first;
            this.mSecond = second;
        }

        public int getFirst() {
            return mFirst;
        }

        public int getSecond() {
            return mSecond;
        }
    }
    
    /**
     * To use sorting reducer
    */
    public static class WordTfKey implements WritableComparable<WordTfKey> {
        // Word
        private Text mFirst;
        // Term frequency
        private IntWritable mSecond;

        public WordTfKey() {
            set(new Text(), new IntWritable());
        }

        public WordTfKey(String first, int second) {
            set(new Text(first), new IntWritable(second));
        }

        public void set(Text first, IntWritable second) {
            this.mFirst = first;
            this.mSecond = second;
        }

        public Text getFirst() {
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
        
        // Distribution by first value of key
        @Override
        public int hashCode() {
            return mFirst.hashCode();
        }

        @Override
        public boolean equals(Object o) {
            if (o instanceof WordTfKey) {
                WordTfKey tmp = (WordTfKey) o;            
                return mFirst.equals(tmp.mFirst) && mSecond.equals(tmp.mSecond);
            }        
            return false;
        }

        @Override
        public String toString() {
            return mFirst + "\t" + mSecond.toString();
        }
        
        // Sort by words and sort by term frequency by descending
        @Override
        public int compareTo(WordTfKey tmp) {
            int cmp = mFirst.compareTo(tmp.mFirst);
            if (cmp != 0) {
                return cmp;
            }
            return -mSecond.compareTo(tmp.mSecond);
        } 
    }

    
    /**
     * input:  key - docid; value - content
     * output: key - (word, tf); value - docid
    */
    public static class MapperInvertedIndex
            extends Mapper<Text, Text, WordTfKey, IntWritable> {
    
        private HighFrequencyTop mTop = new HighFrequencyTop();
        private IntWritable mDocId = new IntWritable();
        private IntWritable mTermFrequency = new IntWritable();
        private Text mTerm = new Text();                         
        private TreeSet<String> mProcessedTerms = new TreeSet();
        private WordTfKey mWordTfKey = new WordTfKey();
         
        
        public void map(Text key, Text value, Context context) 
                throws IOException, InterruptedException {
                        
            StringTokenizer itr = new StringTokenizer(value.toString());            
            mProcessedTerms.clear();
            
            while (itr.hasMoreTokens()) {            
                String term = itr.nextToken().replaceAll("\\p{Punct}", "");

                if (term.length() < 4 || mProcessedTerms.contains(term) || mTop.contains(term)) {
                    continue;
                }

                mProcessedTerms.add(term);                
                int tf = (value.toString() + "\0").split(term).length - 1;   

                if (tf == 0) { 
                    tf = 1;
                }
                
                mDocId.set(Integer.parseInt(key.toString()));
                mTerm.set(term.toLowerCase());
                mTermFrequency.set(tf);                

                mWordTfKey.set(mTerm, mTermFrequency);
                // Send pair and docid
                context.write(mWordTfKey, mDocId);
            }           
        }
    }
    
    /**
     * input:  key - (word, tf); value - docid
     * output: (word, [<docid1, TF-IDF1>, <docid2, TF-IDF2>,..., <docidn, TF-IDFn> ]
    */
    public static class ReducerInvertedIndex 
            extends Reducer<WordTfKey, IntWritable, Text, Text> {
        
        private final int N = 20;
        private int mCnt = 0;
        private String mPreviousTerm = new String();
        private StringBuilder mResult = new StringBuilder();
        private Vector<PairTfDocId> mProcessedTfId = new Vector<PairTfDocId>();        

        public void reduce(WordTfKey key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            
            // mCnt - count documents who have it word
            if (!mPreviousTerm.equals(key.getFirst().toString())) {
                // If not first time         
                if (mCnt != 0) {
                    // Total number of documents determined by a separate MapReduce program
                    float idf = (float)Math.log(86.0F / (float)mCnt);
                    float tf_idf = 0;                   

                    mResult.append("[");

                    for (PairTfDocId val : mProcessedTfId) {
                        tf_idf = val.getFirst() * idf;
                        mResult.append("<" + val.getSecond() + ", " + tf_idf + ">, ");
                    }
                    mResult.deleteCharAt(mResult.length() - 1);
                    mResult.deleteCharAt(mResult.length() - 1);
                    mResult.append("])");
                    
                    context.write(new Text("(" + mPreviousTerm + ","), new Text(mResult.toString()));
                    
                    mResult.delete(0, mResult.length());
                    mCnt = 0;                
                    mProcessedTfId.clear();                    
                }                
            }
            // Stores the first N documents 
            for (IntWritable val : values) {
                if (mCnt < N) {                  
                    mProcessedTfId.add(new PairTfDocId(key.getSecond().get(), val.get()));
                    ++mCnt;
                } else {
                    ++mCnt;
                }               
            }
            mPreviousTerm = key.getFirst().toString();
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        
        if (otherArgs.length != 2) {
            System.err.println("Usage: wordcount <in> <out>");
            System.exit(2);
        }
        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", " ");
        conf.setBoolean("exact.match.only", true);
        conf.set("io.serializations",
                 "org.apache.hadoop.io.serializer.JavaSerialization,"
                 + "org.apache.hadoop.io.serializer.WritableSerialization");
        conf.set("mapreduce.output.textoutputformat.separator", " ");
        
        Job job = new Job(conf, "invertedindex");
        job.setInputFormatClass(XmlInputFormat.class);
        job.setJarByClass(InvertedIndex.class);
        
        job.setMapperClass(MapperInvertedIndex.class);
        job.setReducerClass(ReducerInvertedIndex.class);
        
        job.setMapOutputKeyClass(WordTfKey.class);
        job.setMapOutputValueClass(IntWritable.class);
    
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
} 
