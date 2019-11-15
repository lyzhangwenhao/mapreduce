package com.tom.test;

import org.junit.Test;

import java.util.*;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * ClassName: StreamTest
 * Description:
 *
 * @author Mi_dad
 * @date 2019/11/14 18:01
 */
public class StreamTest {
    @Test
    public void streamTest(){
        List<String> list = new ArrayList<>();
        list.add("5");
        list.add("2");
        list.add("1");
        list.add("4");

        //stream只能够使用一次
        Stream<String> stream = list.stream();



//        Set<String> collect = stream.collect(Collectors.toSet());
//        Iterator<String> iterator = collect.iterator();
//        while (iterator.hasNext()){
//            System.out.println(iterator.next());
//        }

//        Object[] array = stream.toArray();
//        System.out.println("array:===="+ Arrays.toString(array));

//        long count = stream.count();
//        System.out.println("count:===="+count);

//        Stream<String> sorted = stream.sorted();
//        Object[] objects = sorted.toArray();
//        System.out.println(Arrays.toString(objects));

    }
}
