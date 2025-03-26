package com.gulu.spark.rdd.instance;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * @author Gollum
 * @date 2025-03-26 22:46
 */
public class Spark01_Env {
    public static void main(String[] args) {

        // 1.创建配置对象
        final SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("sparkCore");

        // 2. 创建sparkContext
        final JavaSparkContext sc = new JavaSparkContext(conf);



        sc.stop();
        sc.close();
    }
}
