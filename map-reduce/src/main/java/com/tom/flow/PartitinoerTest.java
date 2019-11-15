package com.tom.flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * ClassName: PartitinoerTest
 * Description:
 *
 * @author Mi_dad
 * @date 2019/11/11 21:29
 */
public class PartitinoerTest extends Partitioner<Text,FlowBean> {
    @Override
    public int getPartition(Text key, FlowBean value, int numPartitions) {
        int num = 0;
        String phoneNum = key.toString().substring(0,3);
        if ("135".equals(phoneNum)){
            num = 1;
        }else if("136".equals(phoneNum)){
            num = 2;
        }else if("137".equals(phoneNum)){
            num = 3;
        }else if("138".equals(phoneNum)){
            num = 4;
        }
        return num;
    }
}
