package mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

//                                          k1            v1    k2    v2
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key1, Text value1, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        // 得到数据
        String data = value1.toString();
        String[] words = data.split(" ");

        for (String w : words) {
            context.write(new Text(w), new IntWritable(1));
        }
    }
}
