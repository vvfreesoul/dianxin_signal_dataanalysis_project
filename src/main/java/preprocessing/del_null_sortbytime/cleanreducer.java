/*
* 解决同一时间被不同基站定位的问题
*
* */


package preprocessing.del_null_sortbytime;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.TreeSet;

public class cleanreducer extends Reducer<IntWritable,user_model, NullWritable, Text> {
    TreeSet<user_model> user_set = new TreeSet<user_model>();

    @Override
    protected void reduce(IntWritable key, Iterable<user_model> values, Context context) throws IOException, InterruptedException {
        user_set.clear();
        for(user_model um : values){
            user_model us = new user_model();
            us.setMsisdn(um.getMsisdn());
            us.setUtc_ms(um.getUtc_ms());
            us.setBase_station(um.getBase_station());
            user_set.add(us);
        }
        for (user_model um : user_set){
            context.write(NullWritable.get(),new Text(um.toString()));
        }
    }
}
