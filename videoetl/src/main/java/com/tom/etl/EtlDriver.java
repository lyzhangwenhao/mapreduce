package com.tom.etl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * ClassName: com.tom.etl.EtlDriver
 * Description:
 *
 * @author Mi_dad
 * @date 2019/11/14 22:06
 */
public class EtlDriver{
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        //获取Configuration，Job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //封装Driver
        job.setJarByClass(EtlDriver.class);

        //关联Mapper类
        job.setMapperClass(EtlMapper.class);

        //设置Mapper输出kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        //设置最后输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //输入输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //提交任务
        boolean result = job.waitForCompletion(true);
        System.out.println(result);
    }
}
