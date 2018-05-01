package com.grms.step8;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SaveRecommendResultToDB extends Configured implements Tool{
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf=getConf();
        Path in=new Path(conf.get("in"));

        Job job=Job.getInstance(conf,this.getClass().getSimpleName());
        job.setJarByClass(this.getClass());

        job.setMapperClass(SaveRecommendResultToDBMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,in);

        job.setReducerClass(SaveRecommendResultToDBReducer.class);
        job.setOutputKeyClass(Results.class);
        job.setOutputValueClass(NullWritable.class);
        job.setOutputFormatClass(DBOutputFormat.class);
        DBConfiguration.configureDB(job.getConfiguration(),"com.mysql.jdbc.Driver","jdbc:mysql://192.168.58.131:3306/grms?characterEncoding=UTF-8","root","ROOT");
        DBOutputFormat.setOutput(job,"results","uid","gid","exp");
        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new SaveRecommendResultToDB(),args));
    }
}
