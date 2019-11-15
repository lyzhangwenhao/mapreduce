package com.tom.flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * ClassName: FlowReduce
 * Description:
 *
 * @author Mi_dad
 * @date 2019/11/11 13:59
 */

public class FlowReduce extends Reducer<Text,FlowBean,Text,FlowBean> {
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        long sumUpFlow = 0;
        long sumDownFlow = 0;
        for (FlowBean flowBean:values){
            sumUpFlow += flowBean.getUpFlow();
            sumDownFlow += flowBean.getDownFlow();
        }
        FlowBean flowBean = new FlowBean(sumUpFlow, sumDownFlow);
        context.write(key,flowBean);
    }
}
