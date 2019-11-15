package com.tom.order;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * ClassName: OrderReduce
 * Description:
 *
 * @author Mi_dad
 * @date 2019/11/12 20:33
 */
public class OrderReduce extends Reducer<IntWritable,OrderBean,OrderBean, NullWritable> {
    @Override
    protected void reduce(IntWritable key, Iterable<OrderBean> values, Context context) throws IOException, InterruptedException {
        List<OrderBean> list = new ArrayList<>();
        OrderBean pd = new OrderBean();
        try {
            for (OrderBean orderBean:values){
                //判断是否为订单的文件
                if ("order".equals(orderBean.getFlag())){
                    OrderBean order = new OrderBean();
                    System.out.println(orderBean);
                    BeanUtils.copyProperties(order, orderBean);
                    list.add(order);
                }else {
                    BeanUtils.copyProperties(pd, orderBean);
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        for (OrderBean o:list){
            o.setPname(pd.getPname());
            context.write(o, NullWritable.get());
        }


    }
}
