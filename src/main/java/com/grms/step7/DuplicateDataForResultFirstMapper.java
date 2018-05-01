package com.grms.step7;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/*
        10001	20001	1
        10001	20002	1
        10001	20005	1
        10001	20006	1
        10001	20007	1
        10002	20003	1
 */

/*
        10001:20001  f10001:20001
 */
public class DuplicateDataForResultFirstMapper extends Mapper<LongWritable,Text,Text,Text>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] strs = value.toString().split("\t");
        context.write(new Text(strs[0]+":"+strs[1]),new Text());
    }
}
