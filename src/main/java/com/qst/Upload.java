package com.qst;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;

public class Upload {
    public static void upload(Configuration conf) throws Exception {
        conf.set("fs.defaultFS","hdfs://192.168.150.131:9000");
        URI uri = new URI("hdfs://192.168.150.131:9000");
        FileSystem hdfs = FileSystem.get(uri,conf,"zhyp");
        Path input = new Path("C:\\Users\\26592\\Desktop\\大数据课设\\data_cleaned.csv");
        Path output = new Path("/");
        hdfs.copyFromLocalFile(input, output);
        FileStatus[] file = hdfs.listStatus(output);
        System.out.println(file[0].getPath());
    }
}
