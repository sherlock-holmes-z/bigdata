package com.gulu.spark.rdd.instance;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;

/**
 * @author chocolate
 * 2025/3/27 15:03
 */
public class Spark03_RDD_Disk_Partition {
    public static void main(String[] args) {
        final SparkConf conf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("sparkCore");

        final JavaSparkContext sc = new JavaSparkContext(conf);

        // 操作磁盘文件(可以是绝对路径，也可以是相对路径)
        JavaRDD<String> rdd = sc.textFile("src/main/resources/rdd/1.txt");
        System.out.println(rdd.getNumPartitions());
        rdd.saveAsTextFile("src/main/resources/disk_out3");
        sc.close();
    }
}
