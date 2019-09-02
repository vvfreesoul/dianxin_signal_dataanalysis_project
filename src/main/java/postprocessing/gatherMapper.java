package postprocessing;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.SimpleDateFormat;

public class gatherMapper extends Mapper<LongWritable, Text,IntWritable,Text> {
    String[] fields;
    int user;
    String user_stats;
    String utc_ms;

    public static String Date2TimeStamp(String dateStr, String format) {
        try {
            SimpleDateFormat sdf = new SimpleDateFormat(format);
            return String.valueOf(sdf.parse(dateStr).getTime());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "";
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        fields = value.toString().split(",");
        user = Integer.valueOf(fields[0]);
        utc_ms = Date2TimeStamp(fields[1],"yyyy-MM-dd HH:mm:ss");
//        utc_ms = fields[1];
        user_stats = fields[3]+","+utc_ms+","+fields[2];
        context.write(new IntWritable(user),new Text(user_stats));
    }
}
