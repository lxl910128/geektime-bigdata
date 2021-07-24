package club.projectgaia.bigdata.homework1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * @author luoxiaolong <luoxiaolong@kuaishou.com>
 * Created on 2021-07-14
 */
public class MyMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntArrayWritable> {


    @Override
    public void map(LongWritable longWritable, Text text, OutputCollector<Text, IntArrayWritable> outputCollector, Reporter reporter)
            throws IOException {
        String line = text.toString();
        String[] words = line.split("\t");
        IntArrayWritable intArrayWritable = new IntArrayWritable();
        IntWritable[] intArray = new IntWritable[3];
        intArray[0] = new IntWritable(Integer.valueOf(words[7]));
        intArray[1] = new IntWritable(Integer.valueOf(words[8]));
        intArray[2] = new IntWritable(Integer.valueOf(words[7]) + Integer.valueOf(words[8]));
        intArrayWritable.set(intArray);
        outputCollector.collect(new Text(words[1]), intArrayWritable);
    }
}
