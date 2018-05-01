package com.grms.step1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class UserBuyGoodsListMapper  extends Mapper<LongWritable,Text,Text,Text>{
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] strs = value.toString().split("\t");
        context.write(new Text(strs[0]),new Text(strs[1]));
    }
}
