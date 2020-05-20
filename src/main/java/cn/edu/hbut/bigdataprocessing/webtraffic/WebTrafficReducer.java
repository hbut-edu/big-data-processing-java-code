package cn.edu.hbut.bigdataprocessing.webtraffic;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class WebTrafficReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {

    @Override
    protected void reduce(Text key, Iterable<FloatWritable> value,
                          Context context) throws IOException, InterruptedException {

        List<Float> dayWebTrafficList = new ArrayList<Float>();

        for (FloatWritable number : value) {

            dayWebTrafficList.add(number.get());

        }

        context.write(key, new FloatWritable(Collections.max(dayWebTrafficList)));

    }
}