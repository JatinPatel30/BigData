/**
 * title:  Find the list of Airlines having zero stops
 * Commands :
 * hadoop com.sun.tools.javac.Main ProblemStatement2.java
 * jar cf ProbleStatement.jar ProblemStatement2*.class
 * hadoop jar ProbleStatement.jar ProblemStatement2 projects/project1/Final_airlines projects/project1/routes.dat output-ps2
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class ProblemStatement2 {


    public static class AirlinesMapperClass extends Mapper<LongWritable, Text, LongWritable, Text> {

        private static final Map<Long, String> AIRLINES_CACHE = new HashMap<>();
        private static final LongWritable KEY = new LongWritable();
        private static final Text VALUE = new Text();
        private static final int ID_INDEX = 0;
        private static final int AIRLINE_NAME_INDEX = 1;

        private static final int ROUTE_ID_INDEX = 1;
        private static final int STOPS_INDEX = 7;

        @Override
        protected void setup(Context context) throws java.io.IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            System.out.println("Preaparing cache");
            if (Objects.nonNull(cacheFiles) && cacheFiles.length > 0) {
                try {
                    FileSystem fs = FileSystem.get(context.getConfiguration());
                    Path getFilePath = new Path(cacheFiles[0].toString());
                    String line = "";
                    try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fs.open(getFilePath)))) {
                        while ((line = bufferedReader.readLine()) != null) {
                            String[] columns = line.split(",");
                            AIRLINES_CACHE.put(Long.parseLong(columns[ID_INDEX]), columns[AIRLINE_NAME_INDEX]);
                        }
                        System.out.println("Cache Prepared.");
                    }
                } catch (Exception exception) {
                    System.out.println("Unable to parse cache file.");
                    exception.printStackTrace();
                }
            }
        }

        @Override
        public void map(LongWritable longWritable, Text value, Context context) throws IOException, InterruptedException {
            String[] values = value.toString().split(",");
            System.out.println("Cache Size : " + AIRLINES_CACHE.size());
            try {
                if (!"\\N".equals(values[ROUTE_ID_INDEX])) {
                    long airlineId = Long.parseLong(values[ROUTE_ID_INDEX]);
                    if ("0".equals(values[STOPS_INDEX]) && AIRLINES_CACHE.containsKey(airlineId)) {
                        KEY.set(airlineId);
                        VALUE.set(AIRLINES_CACHE.get(airlineId));
                        AIRLINES_CACHE.remove(airlineId);
                        context.write(KEY, VALUE);
                    }
                }
            } catch (NumberFormatException nfe) {
                System.out.println("Input Line :" + value);
                nfe.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws Exception {

        JobConf jobConf = new JobConf(ProblemStatement2.class);
        jobConf.setJobName("Find the list of Airlines having zero stops");
        jobConf.setJarByClass(ProblemStatement2.class);
        jobConf.setOutputKeyClass(LongWritable.class);
        jobConf.setOutputValueClass(Text.class);
        try {

            FileInputFormat.addInputPath(jobConf, new Path(args[1]));
            Path outputPath = new Path(args[2]);
            FileOutputFormat.setOutputPath(jobConf, outputPath);
            outputPath.getFileSystem(jobConf).delete(outputPath, true);

            Job job = Job.getInstance(jobConf);
            job.setMapperClass(AirlinesMapperClass.class);
            job.addCacheFile(new URI(args[0]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);

        } catch (Exception exception) {
            exception.printStackTrace();
        }
    }
}
