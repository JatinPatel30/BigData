/** title:  Find list of Airports operating in the Country India
 *
 */

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

public class ProblemStatement1 {


    public static class MapClass extends MapReduceBase implements org.apache.hadoop.mapred.Mapper<LongWritable, Text, NullWritable, Text> {
        @Override
        public void map(LongWritable longWritable, Text value, OutputCollector<NullWritable, Text> outputCollector, Reporter reporter) throws IOException {
            String[] values = value.toString().split(",");
            if ("India".equalsIgnoreCase(values[3]))
                outputCollector.collect(NullWritable.get(), new Text(values[1]));
        }
    }

    public static void main(String[] args) throws Exception {
        JobConf jobConf = new JobConf(ProblemStatement1.class);
        jobConf.setJobName("Find list of Airports operating in the Country India");
        jobConf.setJarByClass(ProblemStatement1.class);
        jobConf.setMapperClass(ProblemStatement1.MapClass.class);
        jobConf.setNumReduceTasks(0);

        jobConf.setOutputKeyClass(NullWritable.class);
        jobConf.setOutputValueClass(Text.class);

        jobConf.setInputFormat(org.apache.hadoop.mapred.TextInputFormat.class);
        jobConf.setOutputFormat(org.apache.hadoop.mapred.TextOutputFormat.class);

        org.apache.hadoop.mapred.FileInputFormat.setInputPaths(jobConf, new Path(args[0]));
        org.apache.hadoop.mapred.FileOutputFormat.setOutputPath(jobConf, new Path(args[1]));
        JobClient.runJob(jobConf);
    }
}
