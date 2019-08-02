package preprocessing.del_null_sortbytime;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class cleanmapper extends Mapper<LongWritable, Text, IntWritable,user_model> {
    private String[] fields;
    private String utc_ms;
    private String msisdn;
    private String base_station;
    private long new_utc ;
    user_model user = new user_model();

    private static HashMap<String,Integer> user_mapper;
    private static HashMap<String,Integer> base_mapper;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        String user_path = conf.get("user_path");
        String base_path = conf.get("base_path");
        Path up = new Path(user_path);
        FileSystem fileSystem = up.getFileSystem(conf);
        InputStream in = fileSystem.open(up);
        BufferedReader br = new BufferedReader(new InputStreamReader(in));
        String line = null;
        int i=1;
        user_mapper = new HashMap<String, Integer>();
        while((line=br.readLine())!=null){
            user_mapper.put(line,i++);
        }

        Path bp = new Path(base_path);
        FileSystem fileSystemofbase = bp.getFileSystem(conf);
        InputStream inofbase = fileSystemofbase.open(bp);
        BufferedReader brofbase = new BufferedReader(new InputStreamReader(inofbase));
        String lineofbase = null;
        int j=1;
        base_mapper = new HashMap<String, Integer>();
        while((lineofbase=brofbase.readLine())!=null){
            String base = lineofbase.split("\\s")[0];
            base_mapper.put(base,j++);
        }


        super.setup(context);
    }

    private static String stampToDate(String utc_ss) throws ParseException {
        Pattern pattern = Pattern.compile("^[0-9]*$");
        Matcher matcher = pattern.matcher(utc_ss);
        if(matcher.matches()){
            return utc_ss;
        }else {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            return String.valueOf(sdf.parse(utc_ss).getTime());
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        fields = value.toString().split(",");
        utc_ms = fields[0];
        msisdn = fields[1];
        base_station = fields[2];
        int u = user_mapper.get(msisdn);
        int station = base_mapper.get(base_station);
        try {
            new_utc = Long.valueOf(stampToDate(utc_ms));
        } catch (ParseException e) {
            e.printStackTrace();
        }
        user.setMsisdn(u);
        user.setUtc_ms(new_utc);
        user.setBase_station(station);
        System.out.println(user.toString());
        context.write(new IntWritable(u),user);
    }
}
