package com.grms.step2;

import com.grms.step1.UserBuyGoodsListMapper;
import com.grms.step1.UserBuyGoodsListReduce;
import com.grms.utils.JobUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class GoodsCooCurrentList extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new GoodsCooCurrentList(),args));
    }
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        JobUtil.setConf(conf, "GoodsCooCurrentList", conf.get("in"), conf.get("out"), this.getClass());
        JobUtil.setMapper(GoodsCooCurrentListMapper.class, Text.class,Text.class, TextInputFormat.class);
        JobUtil.setReducer(GoodsCooCurrentListReducer.class, Text.class, Text.class, TextOutputFormat.class);
        return JobUtil.commit();
    }
}
