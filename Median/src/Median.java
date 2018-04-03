
import java.io.IOException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import static java.lang.String.*;

public class Median {
    private static Logger logger = Logger.getLogger("Median");
    public static class Map extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            if(!value.toString().equals("")) {
                int num = Integer.parseInt(value.toString());
                context.write(new IntWritable(num), new IntWritable(num));
                //context.write(new Text("*"),new IntWritable(1));
            }
        }
    }

    public static class MyPartition extends Partitioner<IntWritable, IntWritable> {
        public int getPartition(IntWritable key, IntWritable value, int numPartitions){
            return 0;
        }
    }
    public static class Reduce extends Reducer<IntWritable, IntWritable, Text, Text> {
       List<Integer> list = new ArrayList<>();
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
          //  logger.log(Level.INFO,"Key......"+key.toString();
                    for (IntWritable val : values) {
                        list.add(val.get());
                    }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            int halflistlength = (list.size())/2;
            //Collections.sort(list);
           double  mid1 = list.get(halflistlength-1);
           double mid2 = list.get(halflistlength);
           double median = ((mid1+mid2)/2);
           int min = list.get(0);
           int max = list.get((list.size()-1));
           context.write(new Text(valueOf(min)+"\t"+valueOf(max)),new Text(valueOf(median)));
        }
    }


    //Driver program
    public static void main(String[] args) throws Exception{

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        // get all args
        if (otherArgs.length != 2) {
            System.err.println("Usage: Median <in> <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "Median");
        job.setNumReduceTasks(1);
        job.setJarByClass(Median.class);
        job.setMapperClass(Map.class);
        job.setPartitionerClass(MyPartition.class);
        job.setReducerClass(Reduce.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        // set output value type
        job.setOutputValueClass(Text.class);
        //set the HDFS path of the input data
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        // set the HDFS path for the output
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        //Wait till job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        //int listlength = (list.size())/2;
        //Collections.sort(list);
        //median = list.get(listlength-1);
        // logger.log(Level.INFO,"size::::"+count);
    }
}


































