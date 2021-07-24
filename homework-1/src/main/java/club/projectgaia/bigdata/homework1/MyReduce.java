package club.projectgaia.bigdata.homework1;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 * @author luoxiaolong <luoxiaolong@kuaishou.com>
 * Created on 2021-07-15
 */
public class MyReduce extends MapReduceBase implements Reducer<Text, IntArrayWritable, Text, Text> {

    @Override
    public void reduce(Text text, Iterator<IntArrayWritable> iterator, OutputCollector<Text, Text> outputCollector, Reporter reporter)
            throws IOException {
        int[] ret = new int[3];
        while (iterator.hasNext()) {
            Writable[] next = iterator.next().get();
            ret[0] = ret[0] + ((IntWritable) next[0]).get();
            ret[1] = ret[1] + ((IntWritable) next[1]).get();
            ret[2] = ret[2] + ((IntWritable) next[2]).get();
        }
        outputCollector.collect(text, new Text(String.format("%d\t%d\t%d", ret[0], ret[1], ret[2])));
    }
}
