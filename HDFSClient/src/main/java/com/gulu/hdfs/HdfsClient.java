package com.gulu.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileFilter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

/**
 * @author Gollum
 * @date 2024-12-08 23:20
 */
public class HdfsClient {

    private static final Logger logger = LoggerFactory.getLogger(HdfsClient.class.getName());

    private static FileSystem fs;

    @BeforeAll
    public static void init() throws URISyntaxException, IOException, InterruptedException {

        // 1.连接集群的nameNode地址
        URI uri = new URI("hdfs://hadoop01:8020");
        // 2.创建配置文件
        Configuration configuration = new Configuration();
        configuration.set("dfs.replication", "2"); // 代码中的配置优先级最高

        // 3.获取客户端对象
        fs = FileSystem.get(uri, configuration, "gulu");


    }

    @AfterAll
    public static void close() throws IOException {
        // 关闭资源
        fs.close();
        logger.info("Closing HDFSClient");
    }

    @Test
    public void testMkdir() throws IOException {
        // 创建文件夹
        fs.mkdirs(new Path("/testMkdir"));
    }


    /**
     * 参数优先级
     * 代码中的configuration配置 -》 resource下hdfs-site.xml -》 服务器上的hdfs-site.xml  -》 服务器上的hdfs-default.xml
     */
    @Test
    public void testCopyFromLocal() throws IOException {
        // 上传文件，是否删除源数据，目标路径是否覆盖，源数据路径，目标路径
        fs.copyFromLocalFile(
                false, true, new Path("D:\\test\\2.txt"), new Path("/testMkdir2/demo/test.txt")
        );
        // hdfs-site.xml设置为1，configuration设置为2,最终上传文件的副本数为2
    }


    @Test
    public void testCopyToLocal() throws IOException {
        // 下载文件:  是否删除源文件，源文件路径，本地目标文件路径，  false进行crc校验,会随下载文件产生一个crc文件，true不进行校验
        fs.copyToLocalFile(false, new Path("/testMkdir/1.txt"), new Path("D:\\test\\2.txt"), true);
    }

    @Test
    public void testCopyToRemote() throws IOException {
        // 删除文件/目录： 参数1：删除路径，参数2：是否递归删除，删除的文件夹如果是非空文件夹，不递归删除会报错
        fs.delete(new Path("/testMkdir/1.txt"), false);
    }

    @Test
    public void testRename() throws IOException {
        // 文件的移动/更名 参数1：源文件路径  参数2：目标文件路径
        fs.rename(new Path("/testMkdir/1.txt"), new Path("/testRename/2.txt"));
    }


    @Test
    public void testFileDetails() throws IOException {
        // 获取所有文件信息（不包括文件夹）          参数1：文件路径   参数2：是否递归遍历
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/testMkdir"), false);

        while (listFiles.hasNext()) {
            System.out.println("==============================");
            LocatedFileStatus status = listFiles.next();
            System.out.println(status.getPath());
            System.out.println(status.getLen());
            System.out.println(status.getModificationTime());
            System.out.println(status.getReplication());
            System.out.println(status.getOwner());
            System.out.println(status.getGroup());
            System.out.println(status.getPermission());

            // 文件块所在份分片信息
            BlockLocation[] locations = status.getBlockLocations();
            System.out.println(Arrays.asList(locations));

        }

    }


    @Test
    public void testFileStatus() throws IOException {
        // 判断是文件夹还是文件
        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));

        for (FileStatus fileStatus : fileStatuses) {
            System.out.println("======================");
            System.out.println(fileStatus.getPath().getName());
            System.out.println(fileStatus.isDirectory());
        }
    }


}
