package preprocessing.get_one_day;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import preprocessing.del_null_sortbytime.user_model;

import java.io.IOException;

public class OnedayMapper extends Mapper<LongWritable, Text, IntWritable,user_model> {
    String[] fields;
    private long utc_ms;
    private int msisdn;
    private int base_station;
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        fields = value.toString().split(",");
        msisdn = Integer.valueOf(fields[0]);
        utc_ms = Long.valueOf(fields[1]);
        base_station = Integer.valueOf(fields[2]);
        user_model us = new user_model(msisdn,utc_ms,base_station);
        context.write(new IntWritable(msisdn),us);
    }
}
