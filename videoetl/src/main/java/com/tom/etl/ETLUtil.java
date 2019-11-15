package com.tom.etl;

/**
 * ClassName: com.tom.etl.ETLUtil
 * Description:
 *
 * @author Mi_dad
 * @date 2019/11/14 21:36
 */
public class ETLUtil {
    /**
     * 思路：
     * 1、过滤脏数据
     * 2、去掉类别字段中的空格
     * 3、关联视频中的分隔符
     *
     * @param line
     * @return
     */
    public static String cleanData(String line) {
        String[] split = line.split("\t");
        //首先过滤掉单行数据小于九个的
        if (split.length < 9) {
            return null;
        }
        //去掉字段中的空格,原数据中类别的之间的&符号两边分别有一个空格
        String category = split[3].replaceAll(" ", "");

        //替换掉视频地址里面的\t分隔符
        //使用StringBuffer存储最后清洗后的数据
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < split.length; i++) {
            //判断视频字段之前的表属性
            if (i < 9){
                if(i == split.length-1){
                    sb.append(split[i]);
                }else {
                    sb.append(split[i]).append("\t");
                }
            }else{      //视频字段里面的属性
                if (i==split.length-1){
                    sb.append(split[i]);
                }else {
                    sb.append(split[i]).append("&");
                }
            }
        }
        return sb.toString();
    }
}
