package com.grms.step1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class UserBuyGoodsListReduce extends Reducer<Text,Text,Text,Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder sb = new StringBuilder();
        for (Text i : values) {
            sb.append(i).append(",");
        }
        sb.substring(0,sb.length());
        String s = sb.substring(0,sb.length()-1);
        context.write(key,new Text(String.valueOf(s)));
    }
}
