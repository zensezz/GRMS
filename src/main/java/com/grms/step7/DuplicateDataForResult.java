package com.grms.step7;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DuplicateDataForResult extends Configured implements Tool{
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf=getConf();
        Path in1=new Path(conf.get("in1"));
        Path in2=new Path(conf.get("in2"));
        Path out=new Path(conf.get("out"));

        Job job=Job.getInstance(conf,this.getClass().getSimpleName());
        job.setJarByClass(this.getClass());

        MultipleInputs.addInputPath(job,in1,TextInputFormat.class,DuplicateDataForResultFirstMapper.class);
        MultipleInputs.addInputPath(job,in2,TextInputFormat.class,DuplicateDataForResultSecondMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(DuplicateDataForResultReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,out);

        job.setNumReduceTasks(1);

        return job.waitForCompletion(true)?0:1;
    }
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new DuplicateDataForResult(),args));
    }
}
