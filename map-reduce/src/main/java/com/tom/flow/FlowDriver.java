package com.tom.flow;

import com.tom.hadoop.WordCountDriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * ClassName: FlowDriver
 * Description:
 *
 * @author Mi_dad
 * @date 2019/11/11 14:14
 */
public class FlowDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        //1、获取配置信息以及封装任务
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //2、设置jar加载路径
        job.setJarByClass(WordCountDriver.class);

        //3、设置map和reduce类
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReduce.class);

        //4、设置map输出
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        //5、设置reduce输出
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //新加号码分区
        job.setPartitionerClass(PartitinoerTest.class);
        job.setNumReduceTasks(5);


        //设置小文件合并
        job.setInputFormatClass(CombineTextInputFormat.class);
        CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);
        CombineTextInputFormat.setMinInputSplitSize(job, 2048);

        //6、设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //7、提交
        boolean result = job.waitForCompletion(true);
        System.out.println(result);

    }
}
