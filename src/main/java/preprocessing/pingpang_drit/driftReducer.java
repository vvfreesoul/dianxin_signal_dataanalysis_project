package preprocessing.pingpang_drit;

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
import java.util.TreeSet;

public class driftReducer extends Reducer<IntWritable,user_model, NullWritable, Text> {
    final int T = 60000;            //乒乓效应切换阈值（毫秒）
    final int MINV = 20;        //最小漂移速度（千米每小时）
    final int MIND = 500;       //最小漂移距离（米）
    final int MAXV = 100;       //城市交通最大速度（千米每小时）
    final int M = 3;            //漂移数据判别倍数
    private double time0,time1;
    int i;
    user_model ti0,ti1,ti2;   //选取同一用户的3个相邻状态
    double x0,x1;
    String d0;
    private static HashMap<Integer,Double[]> intddouble_mapper;


    ArrayList<user_model> user_tree = new ArrayList<user_model>();
    TreeSet<user_model> constr = new TreeSet<user_model>();      // 用于排序

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

    @Override
    protected void reduce(IntWritable key, Iterable<user_model> values, Context context) throws IOException, InterruptedException {
        user_tree.clear();
        constr.clear();

        for ( user_model um : values){
            user_model us = new user_model();
            us.setMsisdn(um.getMsisdn());
            us.setUtc_ms(um.getUtc_ms());
            us.setBase_station(um.getBase_station());
            user_tree.add(us);
        }
        Collections.sort(user_tree);

        //去除时间相同基站不同的数据
//        System.out.println(user_tree.get(1));
        ArrayList<Integer> num = new ArrayList<Integer>();
        long re_time = user_tree.get(0).getUtc_ms();
        for (int i=1;i<user_tree.size();i++){
            if(user_tree.get(i).getUtc_ms()==re_time){
                num.add(i);
            }else {
                re_time = user_tree.get(i).getUtc_ms();
            }
        }
        int delt = 0;
        for (int i : num){
//            System.out.println(i);
            user_tree.remove(i-delt);
            delt++;
        }
//        System.out.println(user_tree.get(1));


        for (int i=0;i<user_tree.size()-2;i++){
            ti0 = user_tree.get(i);
            ti1 = user_tree.get(i+1);
            ti2 = user_tree.get(i+2);

            //漂移测试
            System.out.println(intddouble_mapper.get(4764)[0]);




            //去除乒乓效应ABA--AAA
            time0 = (ti1.getUtc_ms() - ti0.getUtc_ms());
            time1 = ti2.getUtc_ms() - ti1.getUtc_ms();
            if(time0<T && time1<T && ti0.getBase_station() == ti2.getBase_station() && ti0.getBase_station() != ti1.getBase_station()){
                ti1.setBase_station(ti0.getBase_station());
            }

            //去除漂移数据
            double lon0 = intddouble_mapper.get(ti0.getBase_station())[0];
            double lat0 = intddouble_mapper.get(ti0.getBase_station())[1];
            double lon1 = intddouble_mapper.get(ti1.getBase_station())[0];
            double lat1 = intddouble_mapper.get(ti1.getBase_station())[1];
            double lon2 = intddouble_mapper.get(ti2.getBase_station())[0];
            double lat2 = intddouble_mapper.get(ti2.getBase_station())[1];
            x0 = Distance(lon0, lat0, lon1, lat1);  //获取第i条数据与第i+1条数据的距离
            x1 = Distance(lon0,lat0,lon2,lat2);     //获取第i条数据与第i+2条数据的距离
            double speed1 = (x0/time0*1000)*3.6;         //获取第i条数据到第i+1条数据的速度

            if(x0<MIND || speed1<MINV){             //如果小于最小漂移距离或者小于最小漂移速度
                constr.add(ti0);
            }else if(speed1>MAXV || x0/x1>M){  //如果i到i+1的速度大于最大交通速度或者i到i+1的距离与i到i+2的距离的比值大于最大距离倍数
                constr.add(ti0);
                i++; //出现漂移数据，跳过这条数据
            }else {  //其他情况，写入第i条数据
                constr.add(ti0);
            }
        }

        for (user_model s : constr){
            context.write(NullWritable.get(),new Text(s.toString()));
        }
    }
}
