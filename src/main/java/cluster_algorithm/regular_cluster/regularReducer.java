package cluster_algorithm.regular_cluster;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import preprocessing.del_null_sortbytime.user_model;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

public class regularReducer extends Reducer<IntWritable,user_model, NullWritable, Text> {
    final double CLUSTER_RADIUS = 1000;         //聚类半径（单位m）
    final long CLUSTER_TIMEINTERVAL = 120;    //聚类时间（单位秒）

    user_model ti0,ti1;
    long start_time,end_time,time_interval,t=0,end;
    double lon0,lat0,lon1,lat1,distance;


    ArrayList<user_model> user_list = new ArrayList<user_model>();
    ArrayList<user_model> cluster_list = new ArrayList<user_model>();

    private static HashMap<Integer,Double[]> intddouble_mapper;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        String intddouble_path = conf.get("intdouble_path");
        Path up = new Path(intddouble_path);
        FileSystem fileSystem = up.getFileSystem(conf);
        InputStream in = fileSystem.open(up);
        BufferedReader br = new BufferedReader(new InputStreamReader(in));
        String line = null;
        int i=1;
        intddouble_mapper = new HashMap<Integer,Double[]>();
        while((line=br.readLine())!=null){
            double longi = Double.valueOf(line.split("\\s")[1]);
            double lati = Double.valueOf(line.split("\\s")[2]);
            Double[] num = {longi,lati};
            intddouble_mapper.put(i++,num);
        }
        super.setup(context);
    }

    //根据经纬度计算距离
    public static double Distance(double long1, double lat1, double long2, double lat2) {
        double a, b, R;
        R = 6378137; // 地球半径
        lat1 = lat1 * Math.PI / 180.0;
        lat2 = lat2 * Math.PI / 180.0;
        a = lat1 - lat2;
        b = (long1 - long2) * Math.PI / 180.0;
        double d;
        double sa2, sb2;
        sa2 = Math.sin(a / 2.0);
        sb2 = Math.sin(b / 2.0);
        d = 2
                * R
                * Math.asin(Math.sqrt(sa2 * sa2 + Math.cos(lat1)
                * Math.cos(lat2) * sb2 * sb2));
        return d;
    }

    @Override
    protected void reduce(IntWritable key, Iterable<user_model> values, Context context) throws IOException, InterruptedException {
        user_list.clear();
        cluster_list.clear();

        for ( user_model um : values){
            user_model us = new user_model();
            us.setMsisdn(um.getMsisdn());
            us.setUtc_ms(um.getUtc_ms());
            us.setBase_station(um.getBase_station());
            user_list.add(us);
        }
        Collections.sort(user_list);

        int cluster = 1;
        int i=0;
        int j;
        while(i<user_list.size()) {
            for (j = i; j < user_list.size(); j++) {
                ti0 = user_list.get(i);      //1551455155,11817
                ti1 = user_list.get(j);
                start_time = ti0.getUtc_ms();
                end_time = ti1.getUtc_ms();
                lon0 = intddouble_mapper.get(ti0.getBase_station())[0];
                lat0 = intddouble_mapper.get(ti0.getBase_station())[1];
                lon1 = intddouble_mapper.get(ti1.getBase_station())[0];
                lat1 = intddouble_mapper.get(ti1.getBase_station())[1];

                distance = Distance(lon0, lat0, lon1, lat1);
                time_interval = Math.abs(end_time - start_time);

                if (distance < CLUSTER_RADIUS) {
                    cluster_list.add(ti1);
                    t = time_interval;
                    end = end_time;
                } else {
                    if (t > CLUSTER_TIMEINTERVAL*1000) {    //TODO 时间要统一
                        for (user_model s : cluster_list) {
                            s.setCluster(cluster);
                            context.write(NullWritable.get(), new Text(s.toStringCluster()));
                        }
//
//
                        cluster_list.clear();
                        cluster++;
//
                    } else {
                        cluster_list.clear();
                    }
//
//                    i = j ;
                    break;
                }
            }
            i=j;
        }

    }
}
