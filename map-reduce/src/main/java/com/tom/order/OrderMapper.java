package com.tom.order;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * ClassName: OrderReduceMapper
 * Description:
 *
 * @author Mi_dad
 * @date 2019/11/12 19:45
 */
public class OrderMapper extends Mapper<LongWritable, Text, IntWritable,OrderBean> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        OrderBean v = new OrderBean();
        String pid = null;

        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        String filename = fileSplit.getPath().getName();
        String[] splits = line.split("\t");
        if(filename.endsWith("order.txt")){
            pid = splits[1];
            v.setPid(Integer.parseInt(pid));
            v.setOid(Integer.parseInt(splits[0]));
            v.setPname("");
            v.setCount(Integer.parseInt(splits[2]));
            v.setFlag("order");
        }else if (filename.endsWith("pd.txt")){
            pid = splits[0];
            v.setPid(Integer.parseInt(pid));
            v.setOid(0);
            v.setCount(0);
            v.setPname(splits[1]);
            v.setFlag("pd");
        }
        context.write(new IntWritable(Integer.parseInt(pid)), v);
    }
}
