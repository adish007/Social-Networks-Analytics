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

public class TaskD {

    /*
    This class takes the list of friends and then outputs a map where key = userID, 1
     */
    public static class FriendMap extends Mapper<Object, Text, Text, Text>{
        protected final Text one = new Text("a");

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {


            String[] vals = value.toString().split(",");
            context.write(new Text(vals[2]), one);
        }
    }

    public static class NameMap extends Mapper<Object, Text, Text, Text>{

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] vals = value.toString().split(",");
            context.write(new Text(vals[0]), new Text(vals[1]));
        }
    }

    public static class NameReduce
            extends Reducer<Text, Text, Text, Text>{

        protected final Text one = new Text("a");

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            Text name = new Text();
            for (Text val : values){

                if (val.toString().equals(one.toString())){
                    sum++;
                } else {
                    name = new Text(val);
                }
            }
            context.write(name, new Text(""+sum));

        }



    }

    public void debug(String args[]) throws Exception{
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "TaskD");
        job.setJarByClass(TaskD.class);
        job.setReducerClass(TaskD.NameReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, TaskD.FriendMap.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TaskD.NameMap.class);
        Path outputPath = new Path(args[2]);
        FileOutputFormat.setOutputPath(job, outputPath);
        outputPath.getFileSystem(conf).delete(outputPath);


        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args)  throws Exception{
        long start = System.currentTimeMillis();
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "TaskD");
        job.setJarByClass(TaskD.class);
        job.setReducerClass(TaskD.NameReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, TaskD.FriendMap.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TaskD.NameMap.class);
        Path outputPath = new Path(args[2]);
        FileOutputFormat.setOutputPath(job, outputPath);
        outputPath.getFileSystem(conf).delete(outputPath);


        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        boolean finished = job.waitForCompletion(true);
        long end = System.currentTimeMillis();
        System.out.println("Total Time: " + ((end-start)/1000));
        System.exit(finished ? 0 : 1);

    }

}
