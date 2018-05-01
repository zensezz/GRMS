package com.grms.utils;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class JobUtil {
    private static Job job;
    private static Path input ;
    private static Path output;

    public static void setConf(Configuration cc, String name ,String in,String out, Class cl ) throws IOException, IOException {
        job = Job.getInstance(cc,name);
        job.setJarByClass(cl);
        input = new Path(in);
        output = new Path(out);
    }
    public static void setCombiner(Class<? extends Reducer> c){
        if (c!=null) {
            job.setCombinerClass(c);
        }
    }
    public static void setMapper(Class<? extends Mapper> mClass,
                                 Class<? extends WritableComparable> keyClass,
                                 Class<? extends WritableComparable> valueClass,
                                 Class<? extends InputFormat> inClass) throws IOException {
        job.setMapperClass(mClass);
        job.setMapOutputKeyClass(keyClass);
        job.setMapOutputValueClass(valueClass);
        job.setInputFormatClass(inClass);
        FileInputFormat.addInputPath(job,input);
    }
    public static void setReducer(
            Class<? extends Reducer> rClass,
            Class<? extends WritableComparable> keyClass,
            Class<? extends WritableComparable> valueClass,
            Class<? extends OutputFormat> outClass){
        job.setReducerClass(rClass);
        job.setOutputKeyClass(keyClass);
        job.setOutputValueClass(valueClass);
        job.setOutputFormatClass(outClass);
        FileOutputFormat.setOutputPath(job,output);
    }
    public static int commit() throws InterruptedException, IOException, ClassNotFoundException{
        return job.waitForCompletion(true)?0:1;
    }
}

