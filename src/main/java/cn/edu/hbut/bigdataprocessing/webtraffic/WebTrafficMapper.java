package cn.edu.hbut.bigdataprocessing.webtraffic;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WebTrafficMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FloatWritable>.Context context)
            throws IOException, InterruptedException {

        String line = value.toString();
        String[] nodeData = line.split(",");

        String hashKey = String.valueOf(line.hashCode());

        for (String dayData : nodeData) {
            context.write(new Text(hashKey), new FloatWritable(Float.parseFloat(dayData)));
        }
    }
}