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

public class TaskF {

    public static class FriendsMapper
            extends Mapper<Object, Text, Text, Text> {
        private final static Text myid = new Text();
        private final static Text friendid = new Text();
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] vals = value.toString().split(",");
            myid.set(vals.get(1));
            friendid.set("F" + vals.get(2));
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
            myidaccess.set(vals.get(1));
            friendaccessid.set("A" + vals.get(2));
            context.write(myidaccess, friendaccessid);
        }
    }

    public static class FriendshipReducer extends Reducer< Text,Text ,Text,Text>{
        @Override
        private static final Text EMPTY_TEXT = Text("");
        private Text tag = new Text();
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            while (values.hasNext()) {
                tag = values.next();
                if (tag.charAt(0) == 'F') {
                    orderList.add(new Text(tag.toString().substring(1)));
                } else if (tag.charAt('F') == 'A') {
                    productList.add(new Text(tag.toString().substring(1)));
                }
            }
            /*            boolean realFriend = true;
            Map<IntWritable, IntWritable> map = new HashMap<>();
            for (IntWritable v : values){
            }*/
        }
    }
}
