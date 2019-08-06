package com.hs.hadoop;


import org.apache.commons.httpclient.URIException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;


import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class Test1 {

    @Test
    public void testMkdris() throws IOException, URISyntaxException, InterruptedException {
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        // 配置在集群上运行
        // configuration.set("fs.defaultFS", "hdfs://hadoop102:9000");
        // FileSystem fs = FileSystem.get(configuration);

        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.188.101:9000"), configuration, "root");

        // 2 创建目录
        fs.mkdirs(new Path("/1108/daxian/banzhang"));

        // 3 关闭资源
        fs.close();


    }

    //文件上传
    @Test
    public void uploadeFile() throws URISyntaxException, IOException, InterruptedException {
        // 1 获取文件系统
        Configuration configuration = new Configuration();

        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.188.101:9000"), configuration, "root");

        fs.copyFromLocalFile(new Path("d:/new1.txt"),new Path("/banzhang.txt"));

        fs.close();

        System.out.println("over");
    }

    //文件下载
    @Test
    public void download() throws URISyntaxException, IOException, InterruptedException {
        // 1 获取文件系统
        Configuration configuration = new Configuration();

        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.188.101:9000"), configuration, "root");

        fs.copyToLocalFile(false,new Path("/banzhang.txt"),new Path("d:/ss.txt"),true);

        fs.close();
        System.out.println("over");
    }

    //文件夹删除
    @Test
    public void removeFile() throws URISyntaxException, IOException, InterruptedException {
        // 1 获取文件系统
        Configuration configuration = new Configuration();

        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.188.101:9000"), configuration, "root");

        fs.delete(new Path("/1108/"),true);

        fs.close();

    }

    //IO流上传文件
    @Test
    public void IOuploadFile() throws URISyntaxException, IOException, InterruptedException {
        // 1 获取文件系统
        Configuration configuration = new Configuration();

        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.188.101:9000"), configuration, "root");

        //2.创建输入流
        FileInputStream fileInputStream = new FileInputStream(new File("d:/ss.txt"));

        //3.获取输出流
        FSDataOutputStream fas = fs.create(new Path("/jay.txt"));

        IOUtils.copyBytes(fileInputStream,fas,configuration);

        IOUtils.closeStream(fileInputStream);
        IOUtils.closeStream(fas);

        fs.close();
    }

}
