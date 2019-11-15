package com.tom.flowsort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * ClassName: FlowSortReduce
 * Description:
 *
 * @author Mi_dad
 * @date 2019/11/12 9:59
 */
public class FlowSortReduce extends Reducer<FlowBean, Text, Text, FlowBean> {
    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        long sumUpFlow = 0;
        long sumDownFlow = 0;
        for (Text text:values){
            context.write(text, key);
        }
    }
}
