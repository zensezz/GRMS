package com.grms.step4;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class UserBuyGoodsVectorReducer extends Reducer<Text,Text,Text,Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder sb = new StringBuilder();
        for (Text value : values) {
            sb.append(value).append(":").append(1).append(",");
        }
        context.write(key,new Text(sb.toString().substring(0,sb.length()-1)));
    }
}
