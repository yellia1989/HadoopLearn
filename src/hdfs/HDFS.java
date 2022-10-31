package hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;

import java.io.IOException;
import java.util.Arrays;

public class HDFS {
    public static void main(String[] args) throws IOException {
        // 指定当前的hadoop用户
        System.setProperty("HADOOP_USER_NAME", "root");

        // 配置参数
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.0.21:9000");

        // 创建客户端
        FileSystem client = FileSystem.get(conf);

        // 创建文件夹
        client.mkdirs(new Path("/test/folder2"));

        // 上传文件
        /*{
            FileInputStream input = new FileInputStream("HDFSLearn.iml");
            FSDataOutputStream output = client.create(new Path("/test/folder2/HDFSLearn.iml"));

            byte[] buffer = new byte[1024];
            int len = 0;
            while ((len = input.read(buffer)) > 0) {
                output.write(buffer, 0, len);
            }

            input.close();
            output.flush();
        }*/

        // 获取文件状态
        {
            FileStatus[] files = client.listStatus(new Path("/test/folder2"));
            for (FileStatus f : files) {
                String dir = f.isDirectory() ? "目录" : "文件";
                String path = f.getPath().toString();
                long len = f.getLen();
                long blockSize = f.getBlockSize();
                System.out.println(dir + " " + len + " bytes, blocksize: " + blockSize +" bytes, path: " + path);

                BlockLocation[] fileBlockLocations = client.getFileBlockLocations(f, 0, len);
                for (BlockLocation loc : fileBlockLocations) {
                    System.out.println("hosts: " + Arrays.toString(loc.getHosts()) + ", names: " + Arrays.toString(loc.getNames()));
                }
            }
        }

        // 删除文件
        Boolean bFlag = client.delete(new Path("/test/folder2/HDFSLearn.iml"), true);
        System.out.println("删除" + (bFlag ? "成功" : "失败"));

        // 下载文件
        /*{
            FSDataInputStream input = client.open(new Path("/test/folder2/HDFSLearn.iml"));
            FileOutputStream output = new FileOutputStream("HDFSLearn.iml2");

            IOUtils.copyBytes(input, output, 1024);

            input.close();
            output.flush();
        }*/

        // 关闭客户端
        client.close();

        // 获取datanode
        //创建分布式文件系统对象
        {
            DistributedFileSystem fs2 = (DistributedFileSystem)FileSystem.get(conf);
            DatanodeInfo[] dataNodeStats = fs2.getDataNodeStats();
            for (DatanodeInfo dataNode : dataNodeStats) {
                System.out.println(dataNode.getHostName() + "\t" +
                        dataNode.getName());
            }
        }
    }
}
