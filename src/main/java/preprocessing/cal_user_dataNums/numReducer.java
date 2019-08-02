package preprocessing.cal_user_dataNums;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import preprocessing.del_null_sortbytime.user_model;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

public class numReducer extends Reducer<IntWritable, user_model, IntWritable, Text> {
    ArrayList<user_model> user_list = new ArrayList<user_model>();
    @Override
    protected void reduce(IntWritable key, Iterable<user_model> values, Context context) throws IOException, InterruptedException {
//        user_list.clear();
//        for ( user_model um : values){
//            user_model us = new user_model();
//            us.setMsisdn(um.getMsisdn());
//            us.setUtc_ms(um.getUtc_ms());
//            us.setBase_station(um.getBase_station());
//            user_list.add(us);
//        }
//        Collections.sort(user_list);
//        String ss = String.valueOf(user_list.size());
//        context.write(key,new Text(ss));
        if(key != new IntWritable(3245)){
            System.out.println();
        }else {
            user_list.clear();
            for ( user_model um : values){
                user_model us = new user_model();
                us.setMsisdn(um.getMsisdn());
                us.setUtc_ms(um.getUtc_ms());
                us.setBase_station(um.getBase_station());
                user_list.add(us);
            }
            Collections.sort(user_list);
            String ss = String.valueOf(user_list.size());
            context.write(key,new Text(ss));

        }
    }
}
