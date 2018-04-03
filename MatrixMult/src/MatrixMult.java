
import java.io.IOException;
import java.io.*;
import java.util.*;
import java.util.logging.*;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

import static java.lang.String.valueOf;

public class MatrixMult {
    private static Logger logger = Logger.getLogger("MatrixMult");
    public static class Map1 extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            String[] lineArray = value.toString().split(", ");
            if(lineArray.length==0){

            }
            else{
                    if(lineArray[0].equals("A"))
                       context.write(new Text(lineArray[2].toString()), new Text(lineArray[0].toString()+"_"+lineArray[1].toString()+"_"+lineArray[3]));
                    if(lineArray[0].equals("B"))
                       context.write(new Text(lineArray[1].toString()), new Text(lineArray[0].toString()+"_"+lineArray[2].toString()+"_"+lineArray[3]));
                }

        }
    }

    public static class Reduce1 extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{

            int j = Integer.parseInt(key.toString());
            String[] ar;
            int a=0,b=0;

            ArrayList<String> valuesArrayA = new ArrayList<String>();
            ArrayList<String> valuesArrayB = new ArrayList<String>();

            for(Text C: values){
                ar = C.toString().split("_");
                if(ar[0].equals("A")){
                    valuesArrayA.add(C.toString());
                }
                if(ar[0].equals("B")){
                    valuesArrayB.add(C.toString());
                }
            }

            String[] tempA,tempB;
            int val1,val2, mult;
            for(String A: valuesArrayA){
               // logger.log(Level.INFO,"A"+A);
                tempA = A.split("_");
                val1 = Integer.parseInt(tempA[2]);
                for(String B: valuesArrayB){
                    tempB = B.split("_");
                    val2 = Integer.parseInt(tempB[2]);
                    mult = val1*val2;
                    context.write(new Text(valueOf(j)), new Text(tempA[1]+"_"+tempB[1]+"_"+valueOf(mult)));
                }
            }
        }
    }

    public static class Map2 extends Mapper<Text, Text, Text, IntWritable>{
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException{
            String[] ikmult = value.toString().split("_");
            int i = Integer.parseInt(ikmult[0]);
            int k = Integer.parseInt(ikmult[1]);
            int map2mult = Integer.parseInt(ikmult[2]);
            context.write(new Text(valueOf(i)+"_"+valueOf(k)), new IntWritable(map2mult));
        }
    }

    public static class Reduce2 extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int value=0,sum=0;
            for(IntWritable val:values){
                sum+=val.get();
            }
            context.write(key,new IntWritable(sum));
        }

    }





    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();

        Job job = new Job(conf, "MatrixMult");
        job.setJarByClass(MatrixMult.class);

        Path in = new Path(args[0]);
        Path out = new Path(args[1]);

        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);

        job.setNumReduceTasks(1);

        job.setMapperClass(Map1.class);
        job.setReducerClass(Reduce1.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);


        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.waitForCompletion(true);

        /***************second job**************/

        Configuration conf2 = new Configuration();

        Job job2 = Job.getInstance(conf2, "MatrixMult");
        job2.setJarByClass(MatrixMult.class);

        Path finalOutput = new Path(args[2]);

        MultipleInputs.addInputPath(job2, out, KeyValueTextInputFormat.class, Map2.class);
        // job2.setPartitionerClass(MatrixMult.MyPartition.class);

        FileOutputFormat.setOutputPath(job2, finalOutput);
        job2.setReducerClass(Reduce2.class);
        job2.setOutputFormatClass(TextOutputFormat.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(IntWritable.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        System.exit(job2.waitForCompletion(true)?0:1);
    }
}