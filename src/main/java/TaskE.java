//package org.example;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.util.*;
public class TaskE {
    public class FavoritesMap extends Mapper<Object, Text, Text, Text>{
        private final Text byWhoO = new Text();
        private final Text whatPagee = new Text();
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            String[] vals = value.toString().split(",");
            // we need to figure out first what page is accessing what other page
            // what distinct pages are accessed
            // how many times each page is accessed

            String byWho = vals[1];
            String whatPage = vals[2];
            byWhoO.set(byWho);
            whatPagee.set(whatPage);
            context.write(byWhoO,whatPagee);
        }
    }
    public static class FavoritesReducer extends Reducer<Text, Text, Text, Text>{
        private Text endResult = new Text();
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
           int pagesAccessCount = 0;
           Set<String> uniquePages = new HashSet<String>();
           for(Text val: values){
               pagesAccessCount++;
               uniquePages.add(val.toString());
           }
           int uniquePagesCounter = uniquePages.size();
           //might need boolean flag
           endResult.set("Total Access:"+ pagesAccessCount + "Distinct Acccess" + uniquePagesCounter);

        }
    }

    public static void main(String[] args) throws Exception {
        String[] input = new String[2];
        //input[0] = local host
        //input[1] = local host
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TaskE");
        job.setJarByClass(TaskE.class);
        job.setMapperClass((FavoritesMap.class));
        job.setReducerClass(TaskE.FavoritesReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);



    }

}

