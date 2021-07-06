/**
 * title:  Find the list of Airlines having zero stops
 * Commands :
 * hadoop com.sun.tools.javac.Main ProblemStatement3.java
 * jar cf ProblemStatement.jar ProblemStatement3*.class
 * hadoop jar ProblemStatement.jar ProblemStatement3 projects/project1/Final_airlines projects/project1/routes.dat output-ps3
 */

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

public class ProblemStatement3 {


    public static class AirlinesMapperClass extends Mapper<LongWritable, Text, Text, Text> {

        private static final Map<String, String> AIRLINES_CACHE = new HashMap<>();
        private static final Text KEY = new Text();
        private static final Text VALUE = new Text();
        private static final int AIRLINE_ID_INDEX = 0;
        private static final int AIRLINE_NAME_INDEX = 1;

        private static final int ROUTES_CODESHARE_INDEX = 6;
        private static final int ROUTES_AIRLINE_ID_INDEX = 1;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            if (Objects.nonNull(cacheFiles) && cacheFiles.length > 0) {
                try {
                    FileSystem fs = FileSystem.get(context.getConfiguration());
                    Path getFilePath = new Path(cacheFiles[0].toString());
                    String line = "";
                    try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fs.open(getFilePath)))) {
                        while ((line = bufferedReader.readLine()) != null) {
                            String[] columns = line.split(",");
                            AIRLINES_CACHE.put(columns[AIRLINE_ID_INDEX], columns[AIRLINE_NAME_INDEX]);
                        }
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

            if ("Y".equalsIgnoreCase(values[ROUTES_CODESHARE_INDEX]) &&
                    AIRLINES_CACHE.containsKey(values[ROUTES_AIRLINE_ID_INDEX])) {
                KEY.set(values[ROUTES_AIRLINE_ID_INDEX]);
                VALUE.set(AIRLINES_CACHE.get(values[ROUTES_AIRLINE_ID_INDEX]));
                AIRLINES_CACHE.remove(values[ROUTES_AIRLINE_ID_INDEX]);
                context.write(KEY, VALUE);
            }

        }
    }

    public static void main(String[] args) throws Exception {

        JobConf jobConf = new JobConf(ProblemStatement3.class);
        jobConf.setJobName("Find the list of Airlines having zero stops");
        jobConf.setJarByClass(ProblemStatement3.class);
        jobConf.setOutputKeyClass(Text.class);
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
