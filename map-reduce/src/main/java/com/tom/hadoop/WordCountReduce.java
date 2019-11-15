package com.tom.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * ClassName: WordCountReduce
 * Description:
 *
 * @author Mi_dad
 * @date 2019/11/11 13:29
 */
public class WordCountReduce extends Reducer<Text,IntWritable,Text,IntWritable> {
    int sum =0;
    IntWritable v = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        sum =0;
        for (IntWritable count:values){
            sum = sum + count.get();
        }
        v.set(sum);
        context.write(key,v);

    }
}
