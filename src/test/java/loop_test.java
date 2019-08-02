import java.util.ArrayList;

public class loop_test {
    public static void main(String[] args) {
        ArrayList<Integer> list = new ArrayList<Integer>();
        for(int i=0;i<100;i++){
            list.add(i);
        }
        System.out.println(list);
        ArrayList<Integer> test_list = new ArrayList<Integer>();
        test_list.add(3);
        test_list.add(30);
        test_list.add(35);
        int idx = 0;
        while (idx<list.size()){
            if(test_list.contains(list.get(idx))){
                idx++;
                continue;
            }else {
                System.out.println(list.get(idx++));
            }

        }
    }
}
