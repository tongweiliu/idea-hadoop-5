package com.it18zhang.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class TestHDFS {

    @Test
    public void mkdir() throws Exception{
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://mycluster");
        conf.set("dfs.permissions","false");
        FileSystem fs = FileSystem.get(conf);
        fs.mkdirs(new Path("/user/centos/myhadoop23"));
    }

    @Test
    public void putFile()throws Exception{
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://mycluster");
        FileSystem fs = FileSystem.get(conf);
        FSDataOutputStream outputStream = fs.create(new Path("/user/centos/myhadoop22/b.txt"));

        outputStream.write("helloworld".getBytes());
        outputStream.write("helloworld".getBytes());
        outputStream.write("helloworld".getBytes());
        outputStream.write("helloworld".getBytes());
        outputStream.write("helloworld".getBytes());
        outputStream.write("helloworld".getBytes());
        outputStream.write("helloworld".getBytes());
        outputStream.write("helloworld".getBytes());
        outputStream.write("helloworld".getBytes());
        outputStream.write("helloworld".getBytes());


        outputStream.close();
    }
}
