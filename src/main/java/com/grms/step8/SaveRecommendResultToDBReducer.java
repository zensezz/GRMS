package com.grms.step8;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SaveRecommendResultToDBReducer extends Reducer<Text,Text,Results,NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String uid = "" ;
        String gid = "" ;
        int exp = 0 ;
        for (Text value : values) {
            String[] s = value.toString().split("\t");
            String[] v = s[0].split(":");
            uid = v[0];
            gid = v[1];
            exp = Integer.parseInt(s[1]);
            context.write(new Results(uid,gid,exp),NullWritable.get());
        }
    }
}
