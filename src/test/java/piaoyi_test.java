import preprocessing.del_null_sortbytime.user_model;

import java.util.ArrayList;

public class piaoyi_test {

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

    public static void main(String[] args) {
        ArrayList<user_model> user_tree = new ArrayList<user_model>();
        user_model us = new user_model(1,2,300);
        user_model us1 = new user_model(1,2,400);
        user_model us2 = new user_model(1,3,500);
        user_model us3 = new user_model(1,3,600);
        user_model us4 = new user_model(1,4,500);
        user_model us5 = new user_model(1,4,600);
        user_tree.add(us);
        user_tree.add(us1);
        user_tree.add(us2);
        user_tree.add(us3);
        user_tree.add(us4);
        user_tree.add(us5);

        long t1  = 1551455155300L;
        long t2 = 1551455165460L;
        long t3 = 1551455174280L;
        long dt1 = t2-t1;
        long dt2 = t3-t2;
        Double distance = Distance(121.423,31.11839,121.424,31.11839);
        System.out.println(distance);
        Double v = distance/dt2*1000;
        System.out.println(v);




    }
}
