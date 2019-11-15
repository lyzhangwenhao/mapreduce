package com.tom.ordermap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

/**
 * ClassName: OrderReduceMapper
 * Description:
 *
 * @author Mi_dad
 * @date 2019/11/12 19:45
 */
public class OrderMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {
    Map<Integer,String> map = new HashMap();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream("pd.txt")));
        String line = null;
        while ((line = br.readLine()) != null){
            String[] split = line.split("\t");
            map.put(Integer.parseInt(split[0]),split[1]);
        }
        br.close();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] splits = line.split("\t");
        OrderBean orderBean = new OrderBean();
        orderBean.setOid(Integer.parseInt(splits[0]));
        orderBean.setPid(Integer.parseInt(splits[1]));
        orderBean.setCount(Integer.parseInt(splits[2]));
        int pid = orderBean.getPid();
        String name = map.get(pid);

        orderBean.setPname(name);

        context.write(orderBean, NullWritable.get());
    }
}
