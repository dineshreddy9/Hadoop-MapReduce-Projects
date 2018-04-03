
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MutualFriends {

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            String[] line = value.toString().split("\t");
            if(line.length==1){

            }
            else{
                String[] friends = line[1].split(",");
                int[] friendPair = new int[2];
                for(int i=0;i<friends.length;i++){
                    friendPair[0] = Integer.parseInt(line[0]);
                    friendPair[1] = Integer.parseInt(friends[i]);
                    Arrays.sort(friendPair);
                    context.write(new Text(friendPair[0] + "," + friendPair[1]), new Text(line[1]));
                }
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            Text[] valuesArray = new Text[2];
            String[] keyArray = new String[2];
            keyArray = key.toString().split(",");
           // if( (keyArray[0].equals("0")&&keyArray[1].equals("4")) || (keyArray[0].equals("20")&&keyArray[1].equals("22939")) || (keyArray[0].equals("1")&&keyArray[1].equals("29826")) || (keyArray[0].equals("6222")&&keyArray[1].equals("19272")) || (keyArray[0].equals("28041")&&keyArray[1].equals("28056"))){
                /*String[] newkeyArray = new String[3];
                newkeyArray[0] = keyArray[0];
                newkeyArray[1] = "\t";
                newkeyArray[2] = keyArray[2];*/
                Iterator<Text> itr = values.iterator();
                int i =0;
                while(itr.hasNext()){
                    valuesArray[i] = new Text(itr.next());
                    i++;
                }

                String[] friendPairList1 = valuesArray[0].toString().split(",");
                String[] friendPairList2 = valuesArray[1].toString().split(",");
                List<String> list = new LinkedList<>();
                for(String friend1: friendPairList1){
                    for(String friend2: friendPairList2){
                        if(friend1.equals(friend2)){
                            list.add(friend1);
                        }
                    }
                }
                String mutualFriends = "";
                for(int j=0; j<list.size(); j++){
                    if(j == list.size() - 1)
                        mutualFriends += list.get(j);
                    else
                        mutualFriends += list.get(j)+ ",";
                }
                context.write(new Text(keyArray[0]+"\t"+keyArray[1]), new Text(mutualFriends));
           // }

        }
    }


    //Driver program
    public static void main(String[] args) throws Exception{

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        // get all args
        if (otherArgs.length != 2) {
            System.err.println("Usage: MutualFriends <in> <out>");
            System.exit(2);
        }
        // create a job with name "mutual friends"
        Job job = new Job(conf, "MutualFriends");
        job.setJarByClass(MutualFriends.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        // uncomment the following line to add the Combiner job.setCombinerClass(Reduce.class);
        // set output key type
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


































