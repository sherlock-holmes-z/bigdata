package com.gulu.join.map_join;

import com.gulu.join.TableBean;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * @author chocolate
 * 2025/2/21 14:40
 */
public class JoinMapper2 extends Mapper<LongWritable, Text, Text, NullWritable> {

    Map<String, String> pdMap = new HashMap<>();

    Text k = new Text();

    @Override
    protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        URI[] cacheFiles = context.getCacheFiles();
        Path path = new Path(cacheFiles[0]);

        // 获取文件系统对象，并开流读取
        FileSystem fileSystem = FileSystem.get(context.getConfiguration());
        FSDataInputStream fis = fileSystem.open(path);

        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fis, StandardCharsets.UTF_8));

        String line;
        while (StringUtils.isNotBlank(line = bufferedReader.readLine())) {
            String[] split = line.split(" ");
            pdMap.put(split[0], split[1]);
        }

        IOUtils.closeStream(fileSystem);
        IOUtils.closeStream(fis);
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        String s = value.toString();
        String[] split = s.split(" ");
        k.set(split[0] + "\t" + pdMap.get(split[1]) + "\t" + split[2]);
        context.write(k, NullWritable.get());
    }
}
