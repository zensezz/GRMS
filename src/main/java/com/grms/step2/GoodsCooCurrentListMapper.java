package com.grms.step2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class GoodsCooCurrentListMapper  extends Mapper<LongWritable,Text,Text,Text>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] strs = value.toString().split("\t");
        String[] s = strs[1].split(",");
        for (String s1 : s) {
            for (String s2 : s) {
                context.write(new Text(s1),new Text(s2));
            }
        }
    }
}
