import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class TaskG {

    public class PageMapper extends Mapper<Text,Text,Text, IntWritable>{
        private final IntWritable out = new IntWritable(-1);
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println("Test");
            String[] vals = value.toString().split(",");
            try {
                new Integer("a").intValue();
                context.write(new Text(""+vals[0]), out);
            } catch (NumberFormatException e){
                context.write(new Text(vals[1]), new IntWritable(new Integer(vals[4]).intValue()));
            }
        }
    }

/*    public class AccessMapper extends Mapper<Text, Text, Text, IntWritable>{


        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String[] vals = value.toString().split(",");

            context.write(new Text(vals[1]), new IntWritable(new Integer(vals[4]).intValue()));

        }
    }*/


    public class ActiveReducer extends Reducer<Text,IntWritable,Text,Text>{

        private final Text inactive = new Text(" Is Inactive");


        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int mostRecent = 0;
            for (IntWritable val : values){
                if (val.get() > mostRecent){
                    mostRecent = val.get();
                }
            }
            if (mostRecent < 800000){
                context.write(key, inactive);
            }

        }
    }


    public void debug(String... args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TaskG");

        job.setMapperClass(PageMapper.class);
        job.setJarByClass(TaskG.class);
        job.setReducerClass(ActiveReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);


        FileInputFormat.addInputPath(job, new Path(args[0]));
        //MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class, AccessMapper.class);
        Path outputPath = new Path(args[2]);

        FileOutputFormat.setOutputPath(job, outputPath);
        outputPath.getFileSystem(conf).delete(outputPath);
        System.exit(job.waitForCompletion(true) ? 0 : 1);


    }

    public static void main(String[] args) throws Exception{
        TaskG task =  new TaskG();
        task.debug("/home/aidan/codinShit/CS4433Project1WORKING/java/MyPage.csv",
                "/home/aidan/codinShit/CS4433Project1WORKING/java/AccessLog.txt",
                "/home/aidan/codinShit/CS4433Project1WORKING/java/output");
    }
}
