package club.projectgaia.bigdata.homework1;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

/**
 * @author luoxiaolong <luoxiaolong@kuaishou.com>
 * Created on 2021-07-15
 */
public class Main {
    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(Main.class);
        conf.setJobName("luoxiaolong-homework1");

        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(IntArrayWritable.class);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(MyMapper.class);
        conf.setReducerClass(MyReduce.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        // 第一个参数表示输入
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        // 第二个输入参数表示输出
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        JobClient.runJob(conf);
    }
}
