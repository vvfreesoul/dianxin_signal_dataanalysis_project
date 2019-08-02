import preprocessing.del_null_sortbytime.user_model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.TreeSet;

public class init_test {
    public static void main(String[] args) {
        System.out.println("let's go");
        user_model user1 = new user_model(10,10,3,4,5);
        user_model user2 = new user_model(3,1,3,4,5);
        user_model user3 = new user_model(3,1,3,4,5);


        ArrayList<user_model> ls = new ArrayList<user_model>();
        ls.add(user1);
        ls.add(user2);
        ls.add(user3);
        for(user_model us : ls){
            System.out.println(us.toString());
        }
        Collections.sort(ls);
        for(user_model us : ls){
            System.out.println(us.toString());
        }

//        HashSet<user_model> user_set = new HashSet<user_model>();
//        user_set.addAll(ls);
//        for(user_model us: user_set){
//            System.out.println(us.toString());
//        }
        TreeSet<user_model> ts = new TreeSet<user_model>();
        ts.add(user1);
        ts.add(user2);
        ts.add(user3);

        for (user_model us:ts){
            System.out.println(us.toString());
        }

    }
}
