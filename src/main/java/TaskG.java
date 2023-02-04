import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class TaskG {

    public static class AccessMapper
            extends Mapper<Object,Text,IntWritable, IntWritable>{
        private final IntWritable out = new IntWritable(-1);
        @Override

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] vals = value.toString().split(",");
            /*int val = new Integer(vals[4]).intValue();
            System.out.println(((Object)val).getClass().getName());*/
            context.write(new IntWritable(new Integer(vals[0]).intValue()), new IntWritable(new Integer(vals[4]).intValue()));
        }
    }

    public static class RecentAccessReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>{
        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int temp;


            IntWritable out = new IntWritable(700000);
            System.out.println("Loops through!");
            for (IntWritable val : values){
                if (val.get() > out.get()){
                    out.set(val.get());
                }
            }
            if (out.get() > 700000){
                System.out.println(key.toString());
                context.write(new IntWritable(key.get()), new IntWritable(1));
            }
        }
    }

  /*  public static class PageMapper extends Mapper<Object, Text, Text, Text>{

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println(PageMapper.class.getName());
            String[] vals = value.toString().split(",");
            context.write(new Text(vals[1]), new IntWritable(new Integer(vals[4]).intValue()));
        }
    }


    public static class ActiveReducer extends Reducer<Text,Text,Text,Text>{

        private final Text inactive = new Text(" Is Inactive");


        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int mostRecent = 0;
            for (Text val : values){
                int v = new Integer(val.toString()).intValue();
                if (v > mostRecent){
                    mostRecent = v;
                }
            }
            if (mostRecent > 800000){
                context.write(key, inactive);
            }

        }
    }*/


    public void debug(String[] args) throws Exception {
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1,"Job1");
        job1.setJarByClass(TaskG.class);
        job1.setNumReduceTasks(1);
        job1.setMapperClass(AccessMapper.class);
        //job1.setCombinerClass(RecentAccessReducer.class);
        job1.setReducerClass(RecentAccessReducer.class);
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        Path outputPath = new Path(args[1]);

        FileOutputFormat.setOutputPath(job1, outputPath);
        outputPath.getFileSystem(conf1).delete(outputPath);
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        System.exit(job1.waitForCompletion(true) ? 0 : 1);
        /*job1.waitForCompletion(true);*/

        /*Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TaskG");
        job.setJarByClass(TaskG.class);
        job.setMapperClass(PageMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setReducerClass(ActiveReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPaths(job, args[0]+","+args[2]);

        MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class, PageMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class, AccessMapper.class);



        Path outputPath2 = new Path(args[3]);

        FileOutputFormat.setOutputPath(job, outputPath2);
        outputPath.getFileSystem(conf).delete(outputPath2);
        FileOutputFormat.setOutputPath(job, new Path(args[3]));*/


        //System.exit(job1.waitForCompletion(true) ? 0 : 1);



    }

    public static void main(String[] args) throws Exception{
        TaskG task =  new TaskG();
        task.debug(new String[]{"/home/aidan/codinShit/CS4433Project1WORKING/java/MyPage.csv",
                "/home/aidan/codinShit/CS4433Project1WORKING/java/AccessLog.csv",
                "/home/aidan/codinShit/CS4433Project1WORKING/java/output"});
    }
}
