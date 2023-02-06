import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
public class TaskE {


    public static class FavMap extends Mapper<Object, Text, Text, Text>{
        private final Text byWhoO = new Text();
        private final Text whatPagee = new Text();
        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            String[] vals = value.toString().split(",");
            // we need to figure out first what page is accessing what other page
            // what distinct pages are accessed
            // how many times each page is accessed
            String byWho = vals[1];
            String whatPage = vals[2];
            byWhoO.set(byWho);
            whatPagee.set(whatPage);
            context.write(byWhoO,whatPagee);        }
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
            endResult.set("Total Access: "+ pagesAccessCount + "\tDistinct Acccess" + uniquePagesCounter);
            context.write(key, endResult);
        }
    }

    public static void main(String[] args) throws Exception {
        long start = System.currentTimeMillis();
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TaskE");
        job.setJarByClass(TaskE.class);
        job.setMapperClass(FavMap.class);
        job.setReducerClass(FavoritesReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        Path outputPath = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outputPath);
        outputPath.getFileSystem(conf).delete(outputPath);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        boolean finished = job.waitForCompletion(true);
        long end = System.currentTimeMillis();
        System.out.println("Total Time: " + ((end-start)/1000));
        System.exit(finished ? 0 : 1);
    }

}