
import java.io.IOException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MeanVariance {

    private static Logger logger = Logger.getLogger("MeanVariance");

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            if(!((value.toString()).equals(""))) {
                long tempvalue = Long.parseLong(value.toString());
                Double square = Math.pow(tempvalue, 2);
            context.write(new Text("mapoutkey"),new Text(value+"_"+square));}
        }
    }

    public static class CombineClass extends Reducer<Text, Text, Text, Text>{
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            Integer count= 0;
            Double sumnum = 0D;
            Double sumnumsq = 0D;
            Iterator<Text> itr = values.iterator();
            while(itr.hasNext()){
                String value_square = itr.next().toString();
                String[] valuesquareArray = value_square.split("_");
                Double num = Double.parseDouble(valuesquareArray[0]);
                Double numsq = Double.parseDouble(valuesquareArray[1]);
                count++;
                sumnum+=num;
                sumnumsq += numsq;
            }
                /*Double avgnum = (sumnum/count);
                Double avgnumsq = (sumnumsq/count);*/
            context.write(new Text("comboutkey"),new Text(count+"_"+sumnum+"_"+sumnumsq));
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            Integer totalCount= 0;
            Double totalsumnum = 0D;
            Double totalsumnumsq = 0D;
            Iterator<Text> itr = values.iterator();
            while(itr.hasNext()){
                String value_sumnum = itr.next().toString();
                logger.log(Level.INFO,"value:::::"+value_sumnum);
                String[] valuesumnumArray = value_sumnum.split("_");
                totalCount+= Integer.parseInt(valuesumnumArray[0]);
                totalsumnum += Double.parseDouble(valuesumnumArray[1]);
                totalsumnumsq += Double.parseDouble(valuesumnumArray[2]);
            }
                Double avgnum = (totalsumnum/totalCount);
                Double avgnumsq = (totalsumnumsq/totalCount);
                Double avgnumwholesq = Math.pow(avgnum,2);
                Double var = (avgnumsq-avgnumwholesq);
            context.write(new Text(avgnum+"\t"),new Text(var.toString()));
        }
    }


    //Driver program
    public static void main(String[] args) throws Exception{

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        // get all args
        if (otherArgs.length != 2) {
            System.err.println("Usage: MeanVariance <in> <out>");
            System.exit(2);
        }
        // create a job with name "mean variance"
        Job job = new Job(conf, "MeanVariance");
        job.setJarByClass(MeanVariance.class);
        job.setMapperClass(Map.class);

        job.setCombinerClass(CombineClass.class);
        job.setReducerClass(Reduce.class);
        // uncomment the following line to add the Combiner job.setCombinerClass(Reduce.class);
        // set output key type
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        // set output value type
        job.setOutputValueClass(Text.class);
        //set the HDFS path of the input data
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        // set the HDFS path for the output
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        //Wait till job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}


































