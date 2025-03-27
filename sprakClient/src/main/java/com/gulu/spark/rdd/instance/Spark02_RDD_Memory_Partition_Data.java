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
public class Spark02_RDD_Memory_Partition_Data {
    public static void main(String[] args) {

        // 1.创建配置对象
        final SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("sparkCore");

//        conf.set("spark.default.parallelism","3");

        final JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> list = Arrays.asList(1,2,3,4,5);

        JavaRDD<Integer> rdd = sc.parallelize(list,1);
        System.out.println(rdd.getNumPartitions());
        rdd.saveAsTextFile("src/main/resources/memory_out1");

        sc.close();
    }
}
