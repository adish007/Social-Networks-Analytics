import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class CanadaCanadaCanada {

    public static class CanadizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final IntWritable one = new IntWritable(1);
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] vals = value.toString().split(",");
            if ("CanadaCanadaCanada".equals(vals[2])){
                context.write(new Text(vals[1] + " LIKES " + vals[4]), new IntWritable(Integer.parseInt(vals[0])));
            }
        }
    }



    public void debug(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "FourA");
        job.setNumReduceTasks(0);
        job.setJarByClass(CanadaCanadaCanada.class);
        job.setMapperClass(CanadizerMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "FourA");
        job.setNumReduceTasks(0);
        job.setJarByClass(CanadaCanadaCanada.class);
        job.setMapperClass(CanadizerMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
