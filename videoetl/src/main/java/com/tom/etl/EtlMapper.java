package com.tom.etl;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * ClassName: ETLMapper
 * Description:
 *
 * @author Mi_dad
 * @date 2019/11/14 21:33
 */
public class EtlMapper extends Mapper<LongWritable, Text,Text, NullWritable> {

    private Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1、获取一行数据
        String line = value.toString();
        //2、清洗数据
        String data = ETLUtil.cleanData(line);

        //3、写出去
        if (StringUtils.isBlank(data)){
            return;
        }
        k.set(data);
        context.write(k, NullWritable.get());

    }
}
