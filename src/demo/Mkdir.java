package demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class Mkdir {
    public static void main(String[] args) throws IOException {

        // 配置参数
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.0.20:9000");

        // 创建客户端
        FileSystem client = FileSystem.get(conf);

        // 创建文件夹
        client.mkdirs(new Path("/test/folder2"));

        // 上传文件

        // 关闭客户端
        client.close();
    }
}
