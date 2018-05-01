package com.grms.step3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public  class GoodsCooccurrenceMatrixReducer extends Reducer<Text,Text,Text,Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuffer sb = new StringBuffer() ;
        Map<String,Integer> numcount = new TreeMap<String, Integer>();
        for (Text value : values) {
            String numcountkey=value.toString();
            Integer numcountvalue=numcount.get(numcountkey);
            if (numcount.containsKey(numcountkey)){
                numcount.put(numcountkey,numcountvalue+1);
            }
            else {
                numcount.put(numcountkey,1);
            }
        }
        for (String s : numcount.keySet()) {
            sb.append(s).append(":").append(numcount.get(s)).append(",");
        }
        context.write(key,new Text(sb.substring(0,sb.length()-1)));
    }
}
