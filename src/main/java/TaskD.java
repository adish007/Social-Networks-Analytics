import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Stack;

public class TaskD {

    /*
    This class takes the list of friends and then outputs a map where key = userID, 1
     */
    public class FriendMap extends Mapper<Object, Text, IntWritable, Text>{
        private final Text one = new Text("a");
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println(this.getClass().getName());

            String[] vals = value.toString().split(",");
            context.write(new IntWritable(new Integer(vals[2]).intValue()), one);
        }
    }

    public class NameMap extends Mapper<Object, Text, IntWritable, Text>{
        private final Text one = new Text("1");

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println(this.getClass().getName());

            String[] vals = value.toString().split(",");
            context.write(new IntWritable(new Integer(vals[0]).parseInt(vals[0])), new Text(vals[1]));
        }
    }

    public class FriendReduce
            extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>{
        private IntWritable result = new IntWritable();
        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            System.out.println(this.getClass().getName());

            int sum = 0;
            for (IntWritable val : values){
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public class NameReduce
            extends Reducer<IntWritable, Text, Text, IntWritable>{
        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            System.out.println(this.getClass().getName());
            String name = "";
            Stack<Text> stack = new Stack<>();

            for (Text i : values){
                if (i.toString().length()<2){
                    stack.push(i);
                } else {
                    name = i.toString();
                }
            }
            result.set(stack.size());
            context.write(new Text(name), result);
        }
    }

    public void debug(String args[]) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TaskD");
        job.setJarByClass(TaskD.class);
        //Map
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        //Job
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //idk about these
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        //job.setNumReduceTasks(1);
        job.setCombinerClass(FriendReduce.class);
        job.setReducerClass(NameReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TaskD.NameMap.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, TaskD.FriendMap.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) {

    }

}
