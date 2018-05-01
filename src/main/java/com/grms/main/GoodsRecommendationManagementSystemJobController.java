package com.grms.main;

import com.grms.step1.UserBuyGoodsList;
import com.grms.step1.UserBuyGoodsListMapper;
import com.grms.step1.UserBuyGoodsListReduce;
import com.grms.step2.GoodsCooCurrentList;
import com.grms.step2.GoodsCooCurrentListMapper;
import com.grms.step2.GoodsCooCurrentListReducer;
import com.grms.step3.GoodsCooccurrenceMatrix;
import com.grms.step3.GoodsCooccurrenceMatrixMappper;
import com.grms.step3.GoodsCooccurrenceMatrixReducer;
import com.grms.step4.UserBuyGoodsVector;
import com.grms.step4.UserBuyGoodsVectorMapper;
import com.grms.step4.UserBuyGoodsVectorReducer;
import com.grms.step5.MultiplyGoodsMatrixAndUserVector;
import com.grms.step5.MultiplyGoodsMatrixAndUserVectorFirstMapper;
import com.grms.step5.MultiplyGoodsMatrixAndUserVectorReducer;
import com.grms.step5.MultiplyGoodsMatrixAndUserVectorSecondMapper;
import com.grms.step6.MakeSumForMultiplication;
import com.grms.step6.MakeSumForMultiplicationMapper;
import com.grms.step6.MakeSumForMultiplicationReducer;
import com.grms.step7.DuplicateDataForResult;
import com.grms.step7.DuplicateDataForResultFirstMapper;
import com.grms.step7.DuplicateDataForResultReducer;
import com.grms.step7.DuplicateDataForResultSecondMapper;
import com.grms.step8.Results;
import com.grms.step8.SaveRecommendResultToDB;
import com.grms.step8.SaveRecommendResultToDBMapper;
import com.grms.step8.SaveRecommendResultToDBReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class GoodsRecommendationManagementSystemJobController extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();

        // 路径
        Path instep1path = new Path(conf.get("in"));
        Path outstep1path = new Path(conf.get("out1"));
        Path outstep2path = new Path(conf.get("out2"));
        Path outstep3path = new Path(conf.get("out3"));
        Path outstep4path = new Path(conf.get("out4"));
        Path outstep5path = new Path(conf.get("out5"));
        Path outstep6path = new Path(conf.get("out6"));
        Path outstep7path = new Path(conf.get("out7"));

        // 作业一 配置

        Job job1 = Job.getInstance(conf,UserBuyGoodsList.class.getSimpleName());
        job1.setJarByClass(UserBuyGoodsList.class);

        job1.setMapperClass(UserBuyGoodsListMapper.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        job1.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job1,instep1path);

        job1.setReducerClass(UserBuyGoodsListReduce.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job1,outstep1path);
        job1.waitForCompletion(true);

        // 作业二配置
        Job job2 = Job.getInstance(conf,GoodsCooCurrentList.class.getSimpleName());
        job2.setJarByClass(GoodsCooCurrentList.class);

        job2.setMapperClass(GoodsCooCurrentListMapper.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job2,outstep1path);

        job2.setReducerClass(GoodsCooCurrentListReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job2,outstep2path);
        job2.waitForCompletion(true);

        // 作业三配置
        Job job3 = Job.getInstance(conf,GoodsCooccurrenceMatrix.class.getSimpleName());
        job3.setJarByClass(GoodsCooccurrenceMatrix.class);

        job3.setMapperClass(GoodsCooccurrenceMatrixMappper.class);
        job3.setMapOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        job3.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job3,outstep2path);

        job3.setReducerClass(GoodsCooccurrenceMatrixReducer.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        job3.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job3,outstep3path);
        job3.waitForCompletion(true);

        // 作业四配置
        Job job4 = Job.getInstance(conf,UserBuyGoodsVector.class.getSimpleName());
        job4.setJarByClass(UserBuyGoodsVector.class);

        job4.setMapperClass(UserBuyGoodsVectorMapper.class);
        job4.setMapOutputKeyClass(Text.class);
        job4.setOutputValueClass(Text.class);
        job4.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job4,instep1path);

        job4.setReducerClass(UserBuyGoodsVectorReducer.class);
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(Text.class);
        job4.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job4,outstep4path);
        job4.waitForCompletion(true);

        // 作业五配置

        Job job5 = Job.getInstance(conf, MultiplyGoodsMatrixAndUserVector.class.getSimpleName());
        job5.setJarByClass(MultiplyGoodsMatrixAndUserVector.class);

        MultipleInputs.addInputPath(job5,outstep3path,TextInputFormat.class,MultiplyGoodsMatrixAndUserVectorFirstMapper.class);
        MultipleInputs.addInputPath(job5,outstep4path,TextInputFormat.class,MultiplyGoodsMatrixAndUserVectorSecondMapper.class);
        job5.setMapOutputKeyClass(Text.class);
        job5.setMapOutputKeyClass(Text.class);

        job5.setReducerClass(MultiplyGoodsMatrixAndUserVectorReducer.class);
        job5.setOutputKeyClass(Text.class);
        job5.setMapOutputValueClass(Text.class);
        job5.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job5,outstep5path);
        job5.waitForCompletion(true);


        // MakeSumForMultiplication
        Job job6 = Job.getInstance(conf,MakeSumForMultiplication.class.getSimpleName());
        job6.setJarByClass(MakeSumForMultiplication.class);

        job6.setMapperClass(MakeSumForMultiplicationMapper.class);
        job6.setMapOutputKeyClass(Text.class);
        job6.setOutputValueClass(Text.class);
        job6.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job6,outstep5path);

        job6.setReducerClass(MakeSumForMultiplicationReducer.class);
        job6.setOutputKeyClass(Text.class);
        job6.setOutputValueClass(Text.class);
        job6.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job6,outstep6path);
        job6.waitForCompletion(true);

        // 作业七配置
        Job job7=Job.getInstance(conf, DuplicateDataForResult.class.getSimpleName());
        job7.setJarByClass(DuplicateDataForResult.class);

        MultipleInputs.addInputPath(job7,instep1path,TextInputFormat.class,DuplicateDataForResultFirstMapper.class);
        MultipleInputs.addInputPath(job7,outstep6path,TextInputFormat.class,DuplicateDataForResultSecondMapper.class);
        job7.setMapOutputKeyClass(Text.class);
        job7.setMapOutputValueClass(Text.class);

        job7.setReducerClass(DuplicateDataForResultReducer.class);
        job7.setOutputKeyClass(Text.class);
        job7.setOutputValueClass(Text.class);
        job7.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job7,outstep7path);
        job7.setNumReduceTasks(1);
        job7.waitForCompletion(true);

        // 作业八配置

        Job job8=Job.getInstance(conf, SaveRecommendResultToDB.class.getSimpleName());
        job8.setJarByClass(SaveRecommendResultToDB.class);

        job8.setMapperClass(SaveRecommendResultToDBMapper.class);
        job8.setMapOutputKeyClass(Text.class);
        job8.setMapOutputValueClass(Text.class);
        job8.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job8,outstep7path);

        job8.setReducerClass(SaveRecommendResultToDBReducer.class);
        job8.setOutputKeyClass(Results.class);
        job8.setOutputValueClass(NullWritable.class);
        job8.setOutputFormatClass(DBOutputFormat.class);
        DBConfiguration.configureDB(job8.getConfiguration(),"com.mysql.jdbc.Driver","jdbc:mysql://192.168.58.131:3306/grms?characterEncoding=UTF-8","root","ROOT");
        DBOutputFormat.setOutput(job8,"results","uid","gid","exp");
         job8.waitForCompletion(true);

         //转变为可控制对象
        ControlledJob cj1 = new ControlledJob(job1.getConfiguration());
        ControlledJob cj2 = new ControlledJob(job2.getConfiguration());
        ControlledJob cj3 = new ControlledJob(job3.getConfiguration());
        ControlledJob cj4 = new ControlledJob(job4.getConfiguration());
        ControlledJob cj5 = new ControlledJob(job5.getConfiguration());
        ControlledJob cj6 = new ControlledJob(job6.getConfiguration());
        ControlledJob cj7 = new ControlledJob(job7.getConfiguration());
        ControlledJob cj8 = new ControlledJob(job8.getConfiguration());

        //对可被控制的作业添加依赖关系
        cj2.addDependingJob(cj1);
        cj3.addDependingJob(cj2);
        cj5.addDependingJob(cj3);
        cj5.addDependingJob(cj4);
        cj6.addDependingJob(cj5);
        cj7.addDependingJob(cj5);
        cj7.addDependingJob(cj6);
        cj8.addDependingJob(cj7);

        //构建JobControl对象，将8个可被控制的作业逐个添加。
        JobControl jc=new JobControl("GRMS");
        jc.addJob(cj1);
        jc.addJob(cj2);
        jc.addJob(cj3);
        jc.addJob(cj4);
        jc.addJob(cj5);
        jc.addJob(cj6);
        jc.addJob(cj7);
        jc.addJob(cj8);
        //构建线程对象，并启动线程，执行作业。
        Thread t=new Thread(jc);
        t.start();
        return 0;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new GoodsRecommendationManagementSystemJobController(),args));
    }
}


/*
yarn jar grms.jar com.grms.main.GoodsRecommendationManagementSystemJobController -Din=/grms/rawdata/* -Dout1=/grms/step1/out -Dout2=/grms/step2/out -Dout3=/grms/step3/out -Dout4=/grms/step4/out -Dout5=/grms/step5/out -Dout6=/grms/step6/out -Dout7=/grms/step7/out
 */