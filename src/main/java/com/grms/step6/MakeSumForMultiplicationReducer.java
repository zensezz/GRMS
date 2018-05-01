package com.grms.step6;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MakeSumForMultiplicationReducer  extends Reducer<Text,Text,Text,Text>{
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int count=0;
        for (Text value : values) {
            count =count + Integer.parseInt(String.valueOf(value));
        }
        context.write(key,new Text(String.valueOf(count)));
    }
}
