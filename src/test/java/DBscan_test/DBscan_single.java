package DBscan_test;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import preprocessing.del_null_sortbytime.user_model;

public class DBscan_single {



    public static ArrayList<user_model> getadjacentPoints(user_model centerPoint, ArrayList<user_model> points,int R,int T){
        ArrayList<user_model> adjacentPoints1 = new ArrayList<user_model>();
        adjacentPoints1.clear();
        for(user_model p : points){
            double distance = centerPoint.getDistance(p);
            long t = Math.abs(p.getUtc_ms()-centerPoint.getUtc_ms());
            if(distance<R && t<=T*1000){
//                p.setVisit(true);
                adjacentPoints1.add(p);
            }
        }
        return adjacentPoints1;
    }

    public static HashMap<Integer,Double[]> get_intdouble(String base_path) throws IOException {
        File filename = new File(base_path); // 要读取以上路径的input。txt文件
        InputStreamReader reader = new InputStreamReader(
                new FileInputStream(filename)); // 建立一个输入流对象reader
        BufferedReader br = new BufferedReader(reader); // 建立一个对象，它把文件内容转成计算机能读懂的语言
        String line = null;
        int i=1;
        HashMap<Integer,Double[]> intddouble_mapper = new HashMap<Integer,Double[]>();
        while((line = br.readLine())!=null){
            double longi = Double.valueOf(line.split("\\s")[1]);
            double lati = Double.valueOf(line.split("\\s")[2]);
            Double[] num = {longi,lati};
            intddouble_mapper.put(i++,num);
        }
        return intddouble_mapper;
    }

    public static String date2TimeStamp(String date_str,String format){
        try {
            SimpleDateFormat sdf = new SimpleDateFormat(format);
            return String.valueOf(sdf.parse(date_str).getTime());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "";
    }

    public static void main(String[] args) throws IOException {
        String pathname = "/Users/apple/Desktop/dianxin_signal_dataanalysis_project/src/test/java/DBscan_test/user9981.txt"; // 绝对路径或相对路径都可以，这里是绝对路径，写入文件时演示相对路径
        File filename = new File(pathname); // 要读取以上路径的input。txt文件
        InputStreamReader reader = new InputStreamReader(
                new FileInputStream(filename)); // 建立一个输入流对象reader
        BufferedReader br = new BufferedReader(reader); // 建立一个对象，它把文件内容转成计算机能读懂的语言
        String line = null;
        String[] fields ;
        int msisdn;
        long utc_ms;
        int base_station;
        double longi;
        double lati;
        HashMap<Integer,Double[]> intddouble_mapper = new HashMap<Integer,Double[]>();
        String basestation_peth = "/Users/apple/Desktop/dianxin_signal_dataanalysis_project/src/test/java/4g_base_station.txt";
        intddouble_mapper = get_intdouble(basestation_peth);

        ArrayList<user_model> points = new ArrayList<user_model>();
        ArrayList<user_model> adjacentPoints = new ArrayList<user_model>();   //寻找所有最近邻
        ArrayList<user_model> re_adjacent = new ArrayList<user_model>(); //寻找最近邻的最近邻
        int radius = 500;   //距离阈值
        int minT  = 300;         //时间阈值
        int MinPts = 25;      //核心对象邻居数

        while ((line = br.readLine())!=null){
            fields = line.split(",");
            msisdn = Integer.valueOf(fields[0]);
            utc_ms = Long.valueOf(date2TimeStamp(fields[1], "yyyy-MM-dd HH:mm:ss"));
            base_station = Integer.valueOf(fields[2]);
            double lo1 = intddouble_mapper.get(base_station)[0];
            double la1 = intddouble_mapper.get(base_station)[1];

            user_model p1 = new user_model(msisdn,utc_ms,base_station,lo1,la1,false,0,false);
            points.add(p1);
        }

        int size = points.size();
        int cluster = 1;
        int idx = 0;
        while (idx<size){
            user_model p = points.get(idx++);
            if (!p.isVisit()) {
                p.setVisit(true);
                adjacentPoints = getadjacentPoints(p, points,radius,minT);
                if (adjacentPoints != null && adjacentPoints.size() < MinPts) {

                    p.setNoised(true);
                } else {
                    p.setCluster(cluster);
                    for (int i = 0; i < adjacentPoints.size(); i++) {
                        user_model pointadjacent = adjacentPoints.get(i);
                        if (!pointadjacent.isVisit()) {
                            pointadjacent.setVisit(true);
                            re_adjacent = getadjacentPoints(pointadjacent, points,radius,minT);
                            if (re_adjacent != null && re_adjacent.size() >= MinPts) {
                                adjacentPoints.addAll(re_adjacent);
                            }

                        }
                        if (pointadjacent.getCluster() == 0) {
                            pointadjacent.setCluster(cluster);

                            if (pointadjacent.isNoised()) {
                                pointadjacent.setNoised(false);
                            }
                            String str = pointadjacent.toStringCluster_date();
                            System.out.println(str);
                        }
                    }
                    cluster++;
                }
            }
        }
    }
}
