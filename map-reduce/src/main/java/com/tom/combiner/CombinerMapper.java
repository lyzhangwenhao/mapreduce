package com.tom.combiner;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * ClassName: CombinerMapper
 * Description:
 *
 * @author Mi_dad
 * @date 2019/11/12 19:20
 */
public class CombinerMapper extends Mapper<LongWritable, Text,Text,IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1、读取每一行
        String line = value.toString();
        //2、切分
        String[] splits = line.split(" ");
        for (String word:splits){
            Text k = new Text(word);
            IntWritable v = new IntWritable(1);
            context.write(k, v);

        }
    }
}
