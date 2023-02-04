import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TaskF {

    /*
    csv     Key     |Val
    Access  accessor|accessee
    Friends Friendee|Friender

    for friendee
        accessed = false
        for accessee
            if friendee == accessee
                accessed = true
        if !accessed
            context.write(acessor, XXXXX)


     */

    public static class FriendshipReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable>{
        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
/*            boolean realFriend = true;
            Map<IntWritable, IntWritable> map = new HashMap<>();
            for (IntWritable v : values){
            }*/
        }
    }
}
