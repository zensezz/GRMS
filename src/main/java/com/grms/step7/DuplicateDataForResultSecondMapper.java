package com.grms.step7;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
/*
            10001:20001	10
			10001:20002	11
			10001:20003	1
			10001:20004	2
			10001:20005	9
			10001:20006	10
			10001:20007	8
			10002:20001	2
			10002:20002	2
			10002:20003	3
 */

/*
        10002:20003	 s10002:20003	3
 */
public class DuplicateDataForResultSecondMapper extends Mapper<LongWritable,Text,Text,Text>{
    @Override
    protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        String[] strs=value.toString().split("[\t]");
        context.write(new Text(strs[0]),new Text(strs[1]));
    }
}