package com.gulu.outputformat;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * @author Gollum
 * @date 2025-02-18 22:54
 */
public class LogRecordWriter extends RecordWriter<Text, NullWritable> {

    private final FSDataOutputStream javaOutput;
    private final FSDataOutputStream bigdataOutput;

    public LogRecordWriter(TaskAttemptContext context) throws IOException {
        FileSystem fileSystem = FileSystem.get(context.getConfiguration());

        javaOutput = fileSystem.create(new Path("src/main/resources/outputformat/output/java.log"));

        bigdataOutput = fileSystem.create(new Path("src/main/resources/outputformat/output/bigdata.log"));
    }

    @Override
    public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException {
        String line = text.toString();
        if ("java".equals(line)) {
            // 不加换行符就会输出为一行
            javaOutput.writeBytes(line + "\n");
        } else {
            bigdataOutput.writeBytes(line+ "\n");
        }
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        IOUtils.closeStream(javaOutput);
        IOUtils.closeStream(bigdataOutput);
    }
}
