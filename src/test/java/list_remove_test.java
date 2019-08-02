import java.util.ArrayList;
import preprocessing.del_null_sortbytime.user_model;

public class list_remove_test {
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


//        去除时间相同基站不同的数据
        System.out.println(user_tree.get(1));
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
            System.out.println(i);
            user_tree.remove(i-delt);
            delt++;
        }
        System.out.println(user_tree.get(1));
    }
}
