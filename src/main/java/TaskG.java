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

public class TaskG {

    public static class AccessMapper
            extends Mapper<Object,Text,IntWritable, IntWritable>{
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] vals = value.toString().split(",");
            context.write(new IntWritable(new Integer(vals[1]).intValue()), new IntWritable(new Integer(vals[4]).intValue()));

        }
    }

    public static class PageMapper extends Mapper<Object, Text, IntWritable, IntWritable>{
        private final static IntWritable zero = new IntWritable();
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] vals = value.toString().split(",");
            context.write(new IntWritable(new Integer(vals[0]).intValue()), zero);
        }
    }

    public static class RecentAccessReducer
            extends Reducer<IntWritable, IntWritable, IntWritable, Text>{

        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int max = -1;
            for (IntWritable i : values){
                int temp = i.get();
                if (temp > max){
                    max = temp;
                }
            }
            if (max < 900000){
                context.write(key, new Text("Is Inactive"));
            }
        }
    }


    public static void debug(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "TaskG");
        job.setJarByClass(TaskG.class);
        job.setReducerClass(RecentAccessReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);


        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, PageMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, AccessMapper.class);
        Path outputPath = new Path(args[2]);
        FileOutputFormat.setOutputPath(job, outputPath);
        outputPath.getFileSystem(conf).delete(outputPath);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception{
        long start = System.currentTimeMillis();
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "TaskG");
        job.setJarByClass(TaskG.class);
        job.setReducerClass(RecentAccessReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);


        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, PageMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, AccessMapper.class);
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
