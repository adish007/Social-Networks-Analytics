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

import java.io.IOException;

public class TaskH {

    static int totalFriendships;
    static int totalPeople;
    static double averageFriendships;

    public static class FriendCountMapper extends Mapper<Object, Text,Text, IntWritable>{
        private final IntWritable one = new IntWritable(1);

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String vals[] = value.toString().split(",");
            totalFriendships++;
            //vals[2] is PersonID
            context.write(new Text(vals[2]), one);
        }
    }

    public static class UserMapper extends Mapper<Object, Text, Text, IntWritable>{
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            totalPeople++;
        }
    }

    public static class FamousReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int i = 0;

            while (values.iterator().hasNext()){
                values.iterator().next();
                i++;
            }
            if (i > averageFriendships && averageFriendships != 0) {
                context.write(key, new IntWritable(i));
            } else if (averageFriendships == 0){
                averageFriendships = totalFriendships/totalPeople;
                if (i > averageFriendships && averageFriendships != 0) {
                    context.write(key, new IntWritable(i));
                }
            }
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        long start = System.currentTimeMillis();
        totalFriendships = 0;
        totalPeople = 0;
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TaskH");
        job.setJarByClass(TaskH.class);
        job.setReducerClass(FamousReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, UserMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, FriendCountMapper.class);
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
