package cn.doitedu.rtdw.utils;

import java.io.*;
import java.util.ArrayList;

public class DimRecProcess {

    public static void main(String[] args) throws IOException {

        BufferedReader br = new BufferedReader(new FileReader(args.length > 0 ? args[0] : RuntimeConfig.value("DIM_REC_INPUT", "d:/dim_red.csv")));
        br.readLine();
        String line ;
        ArrayList<String> lst = new ArrayList<>();

        while((line=br.readLine())!=null){
            String[] split = line.split(",");
            // put 'dim_rec_info','recid_01',
            String command1 = "put 'dim_rec_info', '"+split[0]+"','f:rec_type','"+split[1]+"'";
            String command2 = "put 'dim_rec_info', '"+split[0]+"','f:rec_name','"+split[2]+"'";
            String command3 = "put 'dim_rec_info', '"+split[0]+"','f:rec_suan','"+split[3]+"'";

            lst.add(command1);
            lst.add(command2);
            lst.add(command3);
        }
        br.close();

        // 所有命令全部输出
        BufferedWriter bw = new BufferedWriter(new FileWriter(args.length > 1 ? args[1] : RuntimeConfig.value("DIM_REC_OUTPUT", "d:/ok.sh")));
        for (String cmd : lst) {
            bw.write(cmd);
            bw.newLine();
        }
        bw.close();



    }
}
