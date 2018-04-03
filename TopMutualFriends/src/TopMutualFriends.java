
import java.io.IOException;
import java.io.*;
import java.util.*;
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

public class TopMutualFriends {

    public static class Map1 extends Mapper<LongWritable, Text, Text, Text> {
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

    public static class Reduce1 extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            Text[] valuesArray = new Text[2];
            String[] keyArray = new String[2];
            keyArray = key.toString().split(",");
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
                String TopMutualFriends = "";
                for(int j=0; j<list.size(); j++){
                    if(j == list.size() - 1)
                        TopMutualFriends += list.get(j);
                    else
                        TopMutualFriends += list.get(j)+ ",";
                }
                context.write(key, new Text(TopMutualFriends));

        }
    }

    public static class Map2 extends Mapper<Text, Text, Text, IntWritable>{
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException{
            String[] mutFriendsArray = value.toString().split(",");
            int noofMutual = mutFriendsArray.length;
            context.write(key,new IntWritable(noofMutual));
        }
    }

    public static class Reduce2 extends Reducer<Text, IntWritable, Text, Text>{
        HashMap<String, Integer> hmap = new HashMap<>();

        public void reduce(Text key, Iterable<IntWritable> noofMutual, Context context) throws IOException, InterruptedException {

            for (IntWritable a : noofMutual) {
                int nmut = a.get();
                if (nmut > 0) {
                    hmap.put(key.toString(), nmut);
                }
            }
        }

            public <K, V extends Comparable<? super V>> Map<K, V> sortByDescendingValue(Map<K, V> map) {
                return map.entrySet()
                        .stream()
                        .sorted(Map.Entry.comparingByValue(Collections.reverseOrder()))
                        .collect(Collectors.toMap(
                                Map.Entry::getKey,
                                Map.Entry::getValue,
                                (e1, e2) -> e1,
                                LinkedHashMap::new
                        ));
            }
            protected void cleanup(Context context) throws IOException, InterruptedException {
                hmap = (HashMap) sortByDescendingValue(hmap);
                int i = 0;
                for (Map.Entry<String, Integer> entry : hmap.entrySet()) {
                    if (i == 10) {
                        break;
                    }
                    String[] newkeyArray = entry.getKey().split(",");
                    context.write(new Text(newkeyArray[0]+ "\t" + newkeyArray[1]), new Text(entry.getValue() + ""));
                    i++;
                }
            }
    }



    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();

        Job job = new Job(conf, "TopMutualFriends");
        job.setJarByClass(TopMutualFriends.class);

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

        Job job2 = Job.getInstance(conf2, "TopMutualFriends");
        job2.setJarByClass(TopMutualFriends.class);

        Path finalOutput = new Path(args[2]);

        MultipleInputs.addInputPath(job2, out, KeyValueTextInputFormat.class, Map2.class);
       // job2.setPartitionerClass(TopMutualFriends.MyPartition.class);

        FileOutputFormat.setOutputPath(job2, finalOutput);
        job2.setReducerClass(Reduce2.class);
        job2.setOutputFormatClass(TextOutputFormat.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(IntWritable.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        System.exit(job2.waitForCompletion(true)?0:1);
    }
}