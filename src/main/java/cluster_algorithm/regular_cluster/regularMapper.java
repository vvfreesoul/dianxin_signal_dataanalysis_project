package cluster_algorithm.regular_cluster;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import preprocessing.del_null_sortbytime.user_model;

import java.io.IOException;

public class regularMapper extends Mapper<LongWritable, Text, IntWritable,user_model>{
    String[] fieds;
    private long utc_ms;
    private int msisdn;
    private int base_station;
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        fieds = value.toString().split(",");
        msisdn = Integer.valueOf(fieds[0]);
        utc_ms = Long.valueOf(fieds[1]);
        base_station = Integer.valueOf(fieds[2]);
        user_model us = new user_model(msisdn,utc_ms,base_station);
        context.write(new IntWritable(msisdn),us);
    }
}


