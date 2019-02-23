import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;

import scala.Tuple2;

/**
 * @author majiashu
 * @Date 2017年7月25日
 */

public class wordcount {
    public static void main(String[] args) {

        // 第一步：创建SparkConf对象,设置相关配置信息
        SparkConf conf = new SparkConf();
        conf.setAppName("wordcount");
        conf.setMaster("local");

        // 第二步：创建JavaSparkContext对象，SparkContext是Spark的所有功能的入口
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 第三步：创建一个初始的RDD
        // SparkContext中，用于根据文件类型的输入源创建RDD的方法，叫做textFile()方法
        JavaRDD<String> lines = sc.textFile("D:\\sample2.txt");
        //这里arr代表三个数组。因为map函数执行后产生的是三个数组组成的一个列表，如果打印也是一个数组，
//       lines.map(x->x.split("\n")).foreach(y->{
//            for (String s:y){
//                System.out.println(s);
//            }
//        });

        lines.map(x -> x.split(";")).flatMapToPair(arr -> {
           List<Tuple2<String, Integer>> list = new ArrayList<>();
           int index = 0;
           for (String s : arr) {
               if (index != arr.length - 1) {
                   String key = (arr[index] + "," + arr[index + 1]);
                   list.add(new Tuple2<>(key, 1));
                   index++;
               }
           }
           return list.iterator();
        }).reduceByKey((x, y) -> x + y).foreach(p -> System.out.println(p._1 + p._2));

        sc.close();
    }
}
