package preprocessing.get_one_day;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.http.util.TextUtils;
import preprocessing.del_null_sortbytime.user_model;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

public class OnedayMapper extends Mapper<LongWritable, Text, NullWritable,Text> {
    long date_start = 1551801600000L;
    long date_end = 1551888000000L;
    String[] fields;
    private long utc_ms;
    private int msisdn;
    private int base_station;
    String new_utc_ms;
    String result;

    public String timeTransform(long utc,String formats ){
        if(TextUtils.isEmpty(formats)){
            formats = "yyyy-MM-dd HH:mm:ss";
        }
        String date_string = new SimpleDateFormat(formats, Locale.CHINA).format(new Date(utc));
        return date_string;
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//        fields = value.toString().split(",");
//        msisdn = Integer.valueOf(fields[0]);
//        utc_ms = Long.valueOf(fields[1]);
//        base_station = Integer.valueOf(fields[2]);
//        user_model us = new user_model(msisdn,utc_ms,base_station);
//        if (us.getUtc_ms()>=date_start && us.getUtc_ms()<date_end){
//
//            context.write(new IntWritable(msisdn),new Text(us.toString()));
//        }
        fields = value.toString().split(",");
//        msisdn = Integer.valueOf(fields[0]);
        utc_ms = Long.valueOf(fields[1]);
//        base_station = Integer.valueOf(fields[2]);
        if (utc_ms>=date_start && utc_ms<=date_end){
//            new_utc_ms = timeTransform(utc_ms,"yyyy-MM-dd HH:mm:ss");
            result = fields[0]+","+String.valueOf(utc_ms)+","+fields[2];
            context.write(NullWritable.get(),new Text(result));
        }
    }
}
