package com.grms.step3;

import com.grms.utils.JobUtil;
import org.apache.commons.collections.FastArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.*;

public class GoodsCooccurrenceMatrix  extends Configured implements Tool{

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        JobUtil.setConf(conf, "GoodsCooccurrenceMatrix", conf.get("in"), conf.get("out"), this.getClass());
        JobUtil.setMapper(GoodsCooccurrenceMatrixMappper.class, Text.class,Text.class, TextInputFormat.class);
        JobUtil.setReducer(GoodsCooccurrenceMatrixReducer.class, Text.class, Text.class, TextOutputFormat.class);
        return JobUtil.commit();
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new GoodsCooccurrenceMatrix(),args));
    }
}

