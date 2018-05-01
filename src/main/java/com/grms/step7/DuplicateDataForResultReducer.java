package com.grms.step7;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;



/*
            10001	20004	2
			10001	20003	1
			10002	20002	2
			10002	20007	2
			10002	20001	2
			10002	20005	2
			10003	20006	3
			10003	20005	3
 */

/*
    10001:20001  f10001:20001
    10002:20003	 s10002:20003	3

 */
public class DuplicateDataForResultReducer extends Reducer<Text,Text,Text,Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        ArrayList<String> arrayList=new ArrayList<String>();
        for (Text m:values){
            arrayList.add(m.toString());
        }
        if (arrayList.size()==1){
            context.write(key,new Text(arrayList.get(0)));
        }
    }
}
