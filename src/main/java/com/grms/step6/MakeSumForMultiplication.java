package com.grms.step6;

import com.grms.utils.JobUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MakeSumForMultiplication extends Configured implements Tool{

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        JobUtil.setConf(conf, "MakeSumForMultiplication", conf.get("in"), conf.get("out"), this.getClass());
        JobUtil.setMapper(MakeSumForMultiplicationMapper.class, Text.class,Text.class, TextInputFormat.class);
        JobUtil.setReducer(MakeSumForMultiplicationReducer.class, Text.class, Text.class, TextOutputFormat.class);
        return JobUtil.commit();
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new MakeSumForMultiplication(),args));
    }
}
