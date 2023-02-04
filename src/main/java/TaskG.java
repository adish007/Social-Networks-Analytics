import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class TaskG {

    public static class AccessMapper
            extends Mapper<Object,Text,Text, Text>{
        @Override

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] vals = value.toString().split(",");
/*            int val = new Integer(vals[4]).intValue();
            System.out.println(val);*/
            context.write(new Text(vals[1]), new Text(vals[4]));
        }
    }

    public static class PageMapper extends Mapper<Object, Text, Text, Text>{
        private final static Text neg = new Text("-1");
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] vals = value.toString().split(",");
/*            int val = new Integer(vals[0]).intValue();
            System.out.println(val);*/
            context.write(new Text(vals[0]), neg);
        }
    }

    public static class RecentAccessReducer
            extends Reducer<Text, Text, Text, Text>{

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int max = -2;
            for (Text i : values){
                int temp = new Integer(i.toString()).intValue();
                if (temp > max){
                    max = temp;
                }
            }
            if (max < 800000){
                context.write(key, new Text("Is Inactive"));
            }
        }
    }



    /*
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
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "TaskG");
        job.setJarByClass(TaskG.class);
        job.setReducerClass(RecentAccessReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, AccessMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, PageMapper.class);
        Path outputPath = new Path(args[2]);
        FileOutputFormat.setOutputPath(job, outputPath);
        outputPath.getFileSystem(conf).delete(outputPath);


        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception{
        TaskG task =  new TaskG();
        task.debug(new String[]{"/home/aidan/codinShit/CS4433Project1WORKING/java/MyPage.csv",
                "/home/aidan/codinShit/CS4433Project1WORKING/java/AccessLog.csv",
                "/home/aidan/codinShit/CS4433Project1WORKING/java/output"});
    }
}
