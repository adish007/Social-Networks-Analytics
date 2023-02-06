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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class TaskF {

    public static class FriendsMapper
            extends Mapper<Object, Text, Text, Text> {
        private final static Text myid = new Text();
        private final static Text friendid = new Text();
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] vals = value.toString().split(",");
            myid.set(vals[1]);
            friendid.set("F" + vals[2]);
            context.write(myid, friendid);
        }
    }

    public static class AccessMapper
            extends Mapper<Object, Text, Text, Text> {

        private final static Text myidaccess = new Text();
        private final static Text friendaccessid = new Text();
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] vals = value.toString().split(",");
            myidaccess.set(vals[1]);
            friendaccessid.set("A" + vals[2]);
            context.write(myidaccess, friendaccessid);
        }
    }

    public static class FriendshipReducer extends Reducer< Text,Text ,Text,Text>{
        private static final Text EMPTY_TEXT = new Text("");
        private Text tag = new Text();
        Map<Integer, Object> hasAccessed;
        List<Integer> FriendsList;
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            hasAccessed = new HashMap<>();
            FriendsList = new LinkedList<>();
            while (values.iterator().hasNext()) {
                tag = values.iterator().next();
                if (tag.charAt(0) == 'F') {
                    FriendsList.add(new Integer(tag.toString().substring(1)).intValue());
                } else if (tag.charAt(0) == 'A') {
                    hasAccessed.put(new Integer(tag.toString().substring(1)),new Object());
                }
            }
            ececuteJoinLogic(context, key);
            /*            boolean realFriend = true;
            Map<IntWritable, IntWritable> map = new HashMap<>();
            for (IntWritable v : values){
            }*/
        }

        private void ececuteJoinLogic(Context context, Text key) throws IOException, InterruptedException {
            if(!FriendsList.isEmpty() && !hasAccessed.isEmpty()){
                for (Integer f : FriendsList){
                    if (!hasAccessed.containsKey(f)){
                        context.write(key, new Text("IS A FAKE FRIEND!"));
                        break;
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        long start = System.currentTimeMillis();
        Configuration conf = new Configuration();
        Job j1 = Job.getInstance(conf, "TaskF");
        j1.setJarByClass(TaskF.class);
        j1.setReducerClass(FriendshipReducer.class);
        j1.setOutputKeyClass(Text.class);
        j1.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(j1, new Path(args[0]), TextInputFormat.class, FriendsMapper.class);
        MultipleInputs.addInputPath(j1, new Path(args[1]), TextInputFormat.class, AccessMapper.class);
        Path outputPath = new Path(args[2]);
        FileOutputFormat.setOutputPath(j1, outputPath);
        outputPath.getFileSystem(conf).delete(outputPath);
        FileOutputFormat.setOutputPath(j1, new Path(args[2]));
        boolean finished = j1.waitForCompletion(true);
        long end = System.currentTimeMillis();
        System.out.println("Total Time: " + ((end-start)/1000));
        System.exit(finished ? 0 : 1);
    }
}
