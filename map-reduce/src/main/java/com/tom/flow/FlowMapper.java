package com.tom.flow;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.SplitLocationInfo;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.Arrays;

/**
 * ClassName: FlowMapper
 * Description:
 *
 * @author Mi_dad
 * @date 2019/11/11 13:52
 */
public class FlowMapper extends Mapper<LongWritable, Text,Text,FlowBean> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1、读取每一行
        String line = value.toString();
        //2、按照tab键进行切分，形成数组
        String[] datas = line.split("\t");
//        System.out.println(Arrays.toString(datas));
        //3、筛选手机号上行流量和下行流量
        String phoneNum = datas[1];
        String upFlowStr = datas[datas.length - 3];
        String downFlowStr = datas[datas.length - 2];
        //4、组装输出的kv
        Text k = new Text(phoneNum);
        FlowBean v = new FlowBean(Long.parseLong(upFlowStr), Long.parseLong(downFlowStr));

//        FileSplit inputSplit = (FileSplit)context.getInputSplit();
//        Path path = inputSplit.getPath();
//        SplitLocationInfo[] locationInfo = inputSplit.getLocationInfo();
//        System.out.println("getPath():"+path);

        //5、写出kv
        context.write(k,v);

    }
}
