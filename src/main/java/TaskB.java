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

public class TaskB {

    private static int[] largestValues;
    private static int[] largestID;
    static boolean contains(IntWritable key){
        for (int k : largestID){
           if (k == key.get()){
               return true;
           }
        }
        return false;
    }
    private static void test(){
        for (int i = 0; i < 8; i++){
            System.out.println("Value: " + largestValues[i]+ "\t\t" + "ID: " + largestID[i]);
        }
        System.out.println("\n");
    }
    static void add(int key, int value){
        if (value > largestValues[7]){
            int place = -1;
            for (int i = 0; i < largestValues.length; i++){
                if (largestValues[i] < value){
                    place = i;
                    break;
                }
            }
            int idPlaceholder = largestID[place];

            int placeHolder = largestValues[place];
            largestValues[place] = value;
            largestID[place] = key;
            for (int i = place+1; i < largestValues.length; i++){
                place = largestValues[i];
                largestValues[i] = placeHolder;
                placeHolder = place;
                place = largestID[i];
                largestID[i] = idPlaceholder;
                idPlaceholder = place;
            }
        }
    }

    public static class AccessMap extends Mapper<Object, Text, IntWritable, Text>{
        private static final Text one = new Text("");
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] vals = value.toString().split(",");
            context.write(new IntWritable(new Integer(vals[2]).intValue()), one);
        }
    }

    public static class IntSumReducer
            extends Reducer<IntWritable,Text,IntWritable,Text> {
        private Text result = new Text();
        @Override
        public void reduce(IntWritable key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (Text val : values) {
                sum ++;
            }

            add(key.get(), sum);
            result.set(""+sum);
            context.write(key, result);
        }


    }

    public static class nameMapper extends Mapper<Object, Text, IntWritable, Text>{

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] vals = value.toString().split(",");
            IntWritable id = new IntWritable(new Integer(vals[0]).intValue());
            if (contains(id)) {
                context.write(id, new Text(", " + vals[1] + ", " + vals[2]));
            }
        }
    }


    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        long start = System.currentTimeMillis();
        largestValues = new int[8];
        largestID = new int[8];

        Configuration c1 = new Configuration();
        Job job1 = Job.getInstance(c1, "TaskB1");
        job1.setJarByClass(TaskB.class);
        job1.setReducerClass(IntSumReducer.class);
        job1.setOutputValueClass(Text.class);
        job1.setOutputKeyClass(IntWritable.class);
        job1.setMapperClass(AccessMap.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        Path outputPath = new Path(args[2]);
        FileOutputFormat.setOutputPath(job1, outputPath);
        outputPath.getFileSystem(c1).delete(outputPath);
        FileOutputFormat.setOutputPath(job1, new Path(args[2]));

        job1.waitForCompletion(true);

        Configuration c2 = new Configuration();
        Job job2 = Job.getInstance(c2, "TaskB2");
        job2.setJarByClass(TaskB.class);
        job2.setOutputValueClass(Text.class);
        job2.setOutputKeyClass(IntWritable.class);
        job2.setMapperClass(nameMapper.class);

        FileInputFormat.addInputPath(job2, new Path(args[1]));
        outputPath = new Path(args[3]);
        FileOutputFormat.setOutputPath(job2, outputPath);
        outputPath.getFileSystem(c2).delete(outputPath);
        FileOutputFormat.setOutputPath(job2, new Path(args[3]));
        boolean finished = job2.waitForCompletion(true);
        long end = System.currentTimeMillis();
        System.out.println("Total Time: " + ((end-start)/1000));
        System.exit(finished ? 0 : 1);
    }
}
