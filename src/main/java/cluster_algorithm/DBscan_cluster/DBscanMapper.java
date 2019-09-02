package cluster_algorithm.DBscan_cluster;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import preprocessing.del_null_sortbytime.user_model;

import java.io.IOException;
import java.text.SimpleDateFormat;


public class DBscanMapper extends Mapper<LongWritable, Text, IntWritable, user_model> {
    String[] fields;
    int msisdn;
    long utc_ms;
    int base_station;

    /**
     * 日期格式字符串转换成时间戳
     * @param format 如：yyyy-MM-dd HH:mm:ss
     * @return
     */
    public static String date2TimeStamp(String date_str,String format){
        try {
            SimpleDateFormat sdf = new SimpleDateFormat(format);
            return String.valueOf(sdf.parse(date_str).getTime());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "";
    }


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        fields = value.toString().split(",");
        msisdn = Integer.valueOf(fields[0]);
        utc_ms = Long.valueOf(date2TimeStamp(fields[1], "yyyy-MM-dd HH:mm:ss"));
        base_station = Integer.valueOf(fields[2]);
        user_model point = new user_model();
        point.setMsisdn(msisdn);
        point.setUtc_ms(utc_ms);
        point.setBase_station(base_station);
        context.write(new IntWritable(msisdn),point);
    }
}
