package com.grms.step5;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MultiplyGoodsMatrixAndUserVectorReducer extends Reducer<Text,Text,Text,Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String f = "";
        String v = "";
        String s = "" ;
        String mname,nnmae;
        int mnumber,nnumber;
        for (Text value : values) {
            v = value.toString();
            if (v.substring(0,1).equals("f")){
                f = v.substring(1);
            }
            else {
                s = v;
            }
        }
        String[] f1 = f.split(",");
        String[] v1 = s.split(",");
        for (String m:f1){
            mname=m.split(":")[0];
            mnumber=Integer.parseInt(m.split(":")[1]);
            for (String n:v1){
                nnmae = n.split(":")[0];
                nnumber = Integer.parseInt(n.split(":")[1]);
                context.write(new Text(nnmae+":"+mname), new Text(String.valueOf(mnumber*nnumber)));
            }
        }
    }
}
