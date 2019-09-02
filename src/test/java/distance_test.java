import java.io.*;
import java.util.HashMap;

public class distance_test {

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

    public static void main(String[] args) throws IOException {
        String pathname = "/Users/apple/Desktop/dianxin_signal_dataanalysis_project/src/test/java/4g_base_station.txt"; // 绝对路径或相对路径都可以，这里是绝对路径，写入文件时演示相对路径
        File filename = new File(pathname); // 要读取以上路径的input。txt文件
        InputStreamReader reader = new InputStreamReader(
                new FileInputStream(filename)); // 建立一个输入流对象reader
        BufferedReader br = new BufferedReader(reader); // 建立一个对象，它把文件内容转成计算机能读懂的语言
        String line = null;
//        line = br.readLine();
//        System.out.println(line);
        int i=1;
        HashMap<Integer,Double[]> intddouble_mapper = new HashMap<Integer,Double[]>();
        while((line = br.readLine())!=null){
            double longi = Double.valueOf(line.split("\\s")[1]);
            double lati = Double.valueOf(line.split("\\s")[2]);
            Double[] num = {longi,lati};
            intddouble_mapper.put(i++,num);
        }

        int base1 = 4867;
        int base2 = 5330;

        double lo1 = intddouble_mapper.get(base1)[0];
        double la1 = intddouble_mapper.get(base1)[1];
        double lo2 = intddouble_mapper.get(base2)[0];
        double la2 = intddouble_mapper.get(base2)[1];
        double d = Distance(lo1,la1,lo2,la2);
        System.out.println(d);

        String a = "3";
        String b = "2";
        System.out.println(a.compareTo(b));

    }
}
