package com.gulu.spark.rdd.instance;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * @author chocolate
 * 2025/3/27 15:01
 */
public class Spark02_RDD_Memory_Partition {
    public static void main(String[] args) {

        // 1.创建配置对象
        final SparkConf conf = new SparkConf()
                .setMaster("local")
//                .setMaster("local[2]")// 2表示使用两个核心处理，结果为两个文件；*表示使用当前所有cpu核心处理
                .setAppName("sparkCore");

        conf.set("spark.default.parallelism","3");

        // 2. 创建sparkContext
        final JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> list = Arrays.asList("zhangsan", "lisi", "wangwu");

        // sc操作内存数据,也可以指定使用分区数量
        JavaRDD<String> rdd = sc.parallelize(list);

        rdd.saveAsTextFile("src/main/resources/out3");

        sc.close();
    }
}
