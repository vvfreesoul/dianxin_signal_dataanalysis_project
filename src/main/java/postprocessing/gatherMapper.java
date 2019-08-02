package postprocessing;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class gatherMapper extends Mapper<LongWritable, Text,IntWritable,Text> {
    String[] fields;
    int user;
    String user_stats;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        fields = value.toString().split(",");
        user = Integer.valueOf(fields[0]);
        user_stats = fields[3]+","+fields[1]+","+fields[2];
        context.write(new IntWritable(user),new Text(user_stats));
    }
}
