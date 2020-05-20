package cn.edu.hbut.bigdataprocessing;

import cn.edu.hbut.bigdataprocessing.utility.HDFSOperator;
import cn.edu.hbut.bigdataprocessing.webtraffic.WebTrafficMapper;
import cn.edu.hbut.bigdataprocessing.webtraffic.WebTrafficReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class BigDataProcessing {

    public static void run(String inFilePath, String outFilePath) throws Exception {

        Configuration conf = new Configuration();

        // 需要配置yarn，并开放8032端口
        // conf.set("mapreduce.framework.name", "yarn");
        // conf.set("yarn.resourcemanager.hostname", "192.168.1.5");

        Job job = Job.getInstance(conf);

        // 指定执行类
        job.setJarByClass(BigDataProcessing.class);

        // 指定Mapper
        job.setMapperClass(WebTrafficMapper.class);
        // 指定reducer
        job.setReducerClass(WebTrafficReducer.class);

        // 设置Mapper输出的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FloatWritable.class);

        // 设置reducer的输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        // 指定输入文件的位置
        FileInputFormat.setInputPaths(job, new Path(inFilePath));

        // 指定输出文件夹的位置
        FileOutputFormat.setOutputPath(job, new Path(outFilePath));

        // 提交给yarn
        // job.submit();

        try {
            job.waitForCompletion(true);
        } catch (ClassNotFoundException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        try {

            // HOST上的文件位置
            String sourceFilePath = "/root/share/training_data.csv";

            // 准备存放的HDFS路径
            String inFile = "/input/training_data.csv";
            String inFileUrl = HDFSOperator.HDFS_URI + inFile;

            // 结果输出的文件夹
            String outDir = "/output";
            String outDirUrl = HDFSOperator.HDFS_URI + outDir;

            // 自动输出的文件名为：part-r-00000。
            // m = mapper，表示该文件由mapper生成。
            // r = reducer，表示该文件由reducer生成。
            // 00000为mapper/reducer编号。
            String outFileUrl = outDirUrl + "/part-r-00000";

            // 拷贝HOST上的数据文件直达HDFS
            HDFSOperator.copy(sourceFilePath, inFile);

            // 删除既存的结果
            HDFSOperator.delete(outDir);

            // 运行job
            run(inFileUrl, outDirUrl);

            // 打印结果
            HDFSOperator.print(outFileUrl);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}