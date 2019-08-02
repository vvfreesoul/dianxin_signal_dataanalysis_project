package preprocessing.pingpang_drit;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import preprocessing.del_null_sortbytime.user_model;

import java.io.IOException;

public class driftMapper extends Mapper<LongWritable, Text, IntWritable, user_model> {
    private String[] fields;
    private int user;
    private long datetime;
    private int base;
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        fields = value.toString().split(",");
        user = Integer.valueOf(fields[0]);
        datetime = Long.valueOf(fields[1]);
        base = Integer.valueOf(fields[2]);
        user_model user_data = new user_model(user,datetime,base);
        context.write(new IntWritable(user),user_data);
    }
}
