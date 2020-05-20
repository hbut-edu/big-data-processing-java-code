package cn.edu.hbut.bigdataprocessing.utility;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;

public class HDFSOperator {

    // HDFS文件系统所在master的ip地址和端口
    public static final String HDFS_URI = "hdfs://192.168.1.5:9000";

    // HDFS文件系统所在master的用户名
    public static final String USER = "root";

    public static void upload(String localSrc, String dfsDst) throws IOException, InterruptedException, URISyntaxException {
        FileSystem fs = FileSystem.get(new URI(HDFS_URI), new Configuration(), USER);
        fs.copyFromLocalFile(new Path(localSrc), new Path(dfsDst));
        fs.close();
    }

    public static void download(String dfsSrc, String localDst) throws IOException, InterruptedException, URISyntaxException {
        FileSystem fs = FileSystem.get(new URI(HDFS_URI), new Configuration(), USER);
        fs.copyToLocalFile(new Path(dfsSrc), new Path(localDst));
        fs.close();
    }

    public static void getAllFile() throws IOException, InterruptedException, URISyntaxException {
        Configuration cfg = new Configuration();
        FileSystem fs = FileSystem.get(new URI(HDFS_URI), cfg, USER);
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);

        while (listFiles.hasNext()) {
            LocatedFileStatus status = listFiles.next();
            String name = status.getPath().getName();
            System.out.println(status.getPath() + "  " + name);

        }
    }

    public static void mkdir(String dir) throws IOException, InterruptedException, URISyntaxException {
        Configuration cfg = new Configuration();
        FileSystem fs = FileSystem.get(new URI(HDFS_URI), cfg, USER);
        fs.mkdirs(new Path(dir));
        fs.close();
    }

    public static void mv(String src, String dst) throws IOException, InterruptedException, URISyntaxException {
        Configuration cfg = new Configuration();
        FileSystem fs = FileSystem.get(new URI(HDFS_URI), cfg, USER);
        fs.moveFromLocalFile(new Path(src), new Path(dst));
        // fs.moveToLocalFile(new Path(src), new Path(dst));
        fs.close();
    }

    public static void delete(String path) throws IOException, InterruptedException, URISyntaxException {
        Configuration cfg = new Configuration();
        FileSystem fs = FileSystem.get(new URI(HDFS_URI), cfg, USER);
        fs.delete(new Path(path), true);
        fs.close();
    }

    public static void copy(String sourceFilePath, String inFile) throws Exception {

        FileSystem fs = FileSystem.get(new URI(HDFS_URI), new Configuration(), USER);
        FSDataOutputStream stream = fs.create(new Path(inFile), true);
        InputStream in = new FileInputStream(sourceFilePath);
        IOUtils.copyBytes(in, stream, 1024, true);
        fs.close();

    }

    public static void print(String outFilePath) throws Exception {

        FileSystem fs = FileSystem.get(new URI(HDFS_URI), new Configuration(), USER);

        FSDataInputStream fin = fs.open(new Path(outFilePath));
        IOUtils.copyBytes(fin, System.out, 1024, true);
    }
}