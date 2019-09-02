package cluster_algorithm.DBscan_cluster;

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

public class DBscanReducer extends Reducer<IntWritable,user_model, NullWritable, Text> {

    //设定阈值，距离阈值与时间阈值共同组成时空Eps邻域
    int radius = 500;   //距离阈值
    int minT  = 300;         //时间阈值
    int MinPts = 25;      //核心对象邻居数
    private static HashMap<Integer,Double[]> intdouble_mapper;
    ArrayList<user_model> points = new ArrayList<user_model>();
    ArrayList<user_model> adjacentPoints = new ArrayList<user_model>();   //寻找所有最近邻
    ArrayList<user_model> re_adjacent = new ArrayList<user_model>(); //寻找最近邻的最近邻

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        String intdouble_path = conf.get("intdouble_path");  //获得路径参数
        Path up = new Path(intdouble_path);
        FileSystem fileSystem = up.getFileSystem(conf);
        InputStream in = fileSystem.open(up);
        BufferedReader br = new BufferedReader(new InputStreamReader(in));
        String line = null;
        int i=1;
        intdouble_mapper = new HashMap<Integer,Double[]>();
        while((line=br.readLine())!=null) {
            double longi = Double.valueOf(line.split("\\s")[1]);
            double lati = Double.valueOf(line.split("\\s")[2]);
            Double[] num = {longi, lati};
            intdouble_mapper.put(i++, num);
        }
        super.setup(context);
    }

    public ArrayList<user_model> getadjacentPoints(user_model centerPoint, ArrayList<user_model> points){
        ArrayList<user_model> adjacentPoints1 = new ArrayList<user_model>();
        adjacentPoints1.clear();
        for(user_model p : points){
            double distance = centerPoint.getDistance(p);
            long t = Math.abs(p.getUtc_ms()-centerPoint.getUtc_ms());
            if(distance<radius && t<=minT*1000){
//                p.setVisit(true);
                adjacentPoints1.add(p);
            }
        }
        return adjacentPoints1;
    }

    @Override
    protected void reduce(IntWritable key, Iterable<user_model> values, Context context) throws IOException, InterruptedException {

            points.clear();
            adjacentPoints.clear();
            re_adjacent.clear();
            for (user_model p : values) {
                //1,1551455155,11817,121.3242,31.23123
                p.setLongi(intdouble_mapper.get(p.getBase_station())[0]);
                p.setLati(intdouble_mapper.get(p.getBase_station())[1]);
                user_model p1 = new user_model(p.getMsisdn(), p.getUtc_ms(), p.getBase_station(), p.getLongi(), p.getLati(), p.isVisit(), p.getCluster(), p.isNoised());
                points.add(p1);  //points集合中保存着该用户所有轨迹点
            }
            Collections.sort(points);
            int size = points.size();   //7727
            //System.out.println(size);
            int cluster = 1;
            int idx = 0;
            while (idx < size) { //遍历该用户所有轨迹点
                    //System.out.println("不属于前一类"+idx);
                    user_model p = points.get(idx++);
                    //System.out.println("遍历所有集合点："+idx);
                    //选择一个还未访问的点
                    if (!p.isVisit()) {
                        p.setVisit(true);
                        //获取P在时空Eps邻域内的点集合
                        //adjacentPoints不可能为空，因其本身也算在里面
                        adjacentPoints = getadjacentPoints(p, points);
                        //把邻域中的点数小于MinPts的点设为噪音点
                        if (adjacentPoints != null && adjacentPoints.size() < MinPts) {
                            p.setNoised(true);
                        } else {
                            p.setCluster(cluster);
                            for (int i = 0; i < adjacentPoints.size(); i++) {
                                //System.out.println("单点邻居数："+adjacentPoints.size());
                                user_model pointadjacent = adjacentPoints.get(i);


                                if (!pointadjacent.isVisit()) {
                                    pointadjacent.setVisit(true);
                                    //System.out.println(pointadjacent.isVisit());
                                    re_adjacent = getadjacentPoints(pointadjacent, points);
                                    //System.out.println("核心对象邻居的邻居数："+AdjacentadjacentPoints.size());
                                    //System.out.println("re_adjacent大小为" + re_adjacent.size());
                                    if (re_adjacent != null && re_adjacent.size() >= MinPts) {
//                                    for (user_model pp : re_adjacent) {
//                                        if (!adjacentPoints.contains(pp)) {
//                                            adjacentPoints.add(pp);
//                                        }
//                                    }
                                        adjacentPoints.addAll(re_adjacent);
                                        //System.out.println("更新后re_adjacent大小为" + re_adjacent.size());
                                        //System.out.println("更新后的邻居数："+adjacentPoints.size());
                                    }

                                }


                                if (pointadjacent.getCluster() == 0) {
                                    pointadjacent.setCluster(cluster);

                                    if (pointadjacent.isNoised()) {
                                        pointadjacent.setNoised(false);
                                    }
                                    String str = pointadjacent.toStringCluster_date();
                                    //System.out.println(str);
                                    context.write(NullWritable.get(), new Text(str));
                                }
                                //System.out.println(adjacentPoints.size());
                            }
                            //System.out.println(cluster+"--------------对应-----------"+adjacentPoints.size());
                            //cout+=adjacentPoints.size();
                            //System.out.println(cout);
                            cluster++;
                        }


                    }

                }

    }
}
