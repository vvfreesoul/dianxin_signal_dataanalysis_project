package preprocessing.data_modify;

import java.io.*;

public class data_plus_zero {
    public static void main(String[] args) throws IOException {
        String filePath = "data/temp/driftout.txt";
        String fileoutpath = "data/temp/new_driftout.txt";
//        String filePath = "data/temp/test.txt";
//        String fileoutpath = "data/temp/new_test.txt";
        FileInputStream fin = new FileInputStream(filePath);
        InputStreamReader reader = new InputStreamReader(fin);
        BufferedReader br = new BufferedReader(reader);

        BufferedWriter out = new BufferedWriter(new FileWriter(fileoutpath));

        String line = "";
        String[] fields;
        int i = 0;
        while((line = br.readLine())!=null){
            i++;
            if (i%2000000 == 0){
                System.out.println(i);
            }
            fields = line.split(",");
            String new_line = fields[0]+","+fields[1]+"000,"+fields[2];
            out.write(new_line+"\n");
        }
        br.close();
        out.close();
    }
}
