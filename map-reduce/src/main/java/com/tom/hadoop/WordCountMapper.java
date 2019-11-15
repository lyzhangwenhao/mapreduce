package com.tom.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * ClassName: WordCountMapper
 * Description:
 *
 * @author Mi_dad
 * @date 2019/11/11 11:01
 */
public class WordCountMapper extends Mapper<LongWritable, Text,Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1、那倒每行的内容 例如：hello World
        String line = value.toString();
        //2、将每行的内容按照空格切分，形成数组：[hello,world]
        String[] words = line.split(" ");
        //3、遍历数组，将单词和1组装为kv对

        for (String word:words){
            Text k = new Text(word);
            IntWritable v = new IntWritable(1);
            context.write(k,v);
        }
    }
}
