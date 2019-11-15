package com.tom.flowsort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * ClassName: FlowSortMapper
 * Description:
 *
 * @author Mi_dad
 * @date 2019/11/12 9:58
 */
public class FlowSortMapper extends Mapper<LongWritable, Text,FlowBean,Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1、读取每一行
        String line = value.toString();
        //2、将数据按照规则切分
        String[] splits = line.split("\t");
        String phoneNum = splits[0];
        String upFlow = splits[1];
        String downFlow = splits[2];

        FlowBean flowBean = new FlowBean(Long.parseLong(upFlow), Long.parseLong(downFlow));
        Text v = new Text(phoneNum);

        context.write(flowBean, v);
    }
}
