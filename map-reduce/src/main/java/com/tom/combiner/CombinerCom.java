package com.tom.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * ClassName: CombinerReduce
 * Description:
 *
 * @author Mi_dad
 * @date 2019/11/12 19:30
 */
public class CombinerCom extends Reducer<Text, IntWritable,Text,IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum =0;
        IntWritable v = new IntWritable();
        for (IntWritable value:values){
            sum =sum + value.get();
            System.out.println(value.get());
        }
        v.set(sum);
        context.write(key, v);
    }
}
