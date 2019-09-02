/*
解决单个聚类无法输出基站的问题

*/

package postprocessing;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.http.util.TextUtils;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

public class gatherReducer extends Reducer<IntWritable, Text, NullWritable,Text> {
    String keygen,start_time,end_time,utc_ms,base_station,max_key;
    int cluster,mycluster,max;

    Map<String,Integer> map = new HashMap<String, Integer>();

    public String timeTransform(long utc ){
        String formats = "yyyy-MM-dd HH:mm:ss";
        if(TextUtils.isEmpty(formats)){
            formats = "yyyy-MM-dd HH:mm:ss";
        }
        String date_string = new SimpleDateFormat(formats, Locale.CHINA).format(new Date(utc));
        return date_string;
    }




    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<String> list = new ArrayList<String>();
        list.clear();

        keygen = key.toString();
        for(Text p : values){
            list.add(p.toString());  //1,1551660888,1249  聚类，时间，基站    也可能是70,1551660888,1249
        }
        //1,1551368717,9615

        Collections.sort(list, new Comparator<String>() {
            public int compare(String o1, String o2) {
                int c1 = Integer.valueOf(o1.split(",")[0]);
                long t1 = Long.valueOf(o1.split(",")[1]);
                int c2 = Integer.valueOf(o2.split(",")[0]);
                long t2 = Long.valueOf(o2.split(",")[1]);
                if(c1 == c2){
                    if(t1-t2<0){
                        return -1;
                    }else if (t1-t2>0) {
                        return 1;
                    }else {
                        return 0;
                    }
                }else {
                    return c1 - c2;
                }

            }});

        cluster=Integer.valueOf(list.get(0).split(",")[0]);

        start_time = list.get(0).split(",")[1];
        for (int i = 0; i < list.size(); i++) {
            mycluster = Integer.valueOf(list.get(i).split(",")[0]);
            utc_ms = list.get(i).split(",")[1];
            base_station = list.get(i).split(",")[2];
            if (mycluster == cluster) {
                end_time = utc_ms;
                if (!map.containsKey(base_station)) {
                    map.put(base_station, 1);
                } else {
                    map.put(base_station, map.get(base_station) + 1);
                }
            } else {
                max = 0;
                max_key = base_station;
                for (String s : map.keySet()) {
                    if (map.get(s) > max) {
                        max = map.get(s);
                        max_key = s;
                    }
                }
                context.write(NullWritable.get(), new Text(keygen + "," + cluster + "," + max_key + "," + timeTransform(Long.valueOf(start_time)) + "," + timeTransform(Long.valueOf(end_time))));
                map.clear();
                start_time = utc_ms;
                cluster = mycluster;
            }
        }
        max = 0;
        max_key = base_station;
        for (String s : map.keySet()) {
            if (map.get(s) > max) {
                max = map.get(s);
                max_key = s;
            }
        }

        context.write(NullWritable.get(), new Text(keygen + "," + cluster + "," + max_key + "," +timeTransform(Long.valueOf(start_time)) + "," + timeTransform(Long.valueOf(end_time))));
        map.clear();
    }
}
