import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class bai3 {
    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.exit(-1);
        }
        Configuration conf = new Configuration();
        conf.set("users.path", args[2]);
        Job job = Job.getInstance(conf, "Gender Ratings");
        job.setJarByClass(bai3.class);
        job.setMapperClass(GenderMapper.class);
        job.setReducerClass(GenderReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[3]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

class GenderMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Map<String, String> userGender = new HashMap<>();
    private Map<String, String> movieTitle = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        FileSystem fs = FileSystem.get(conf);

        Path usersPath = new Path(conf.get("users.path"));
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(usersPath)));
        String line;
        while ((line = br.readLine()) != null) {
            String[] fields = line.split(",");
            if (fields.length >= 2) {
                userGender.put(fields[0].trim(), fields[1].trim());
            }
        }
        br.close();

        Path moviesPath = new Path("/input/movies.txt");
        br = new BufferedReader(new InputStreamReader(fs.open(moviesPath)));
        while ((line = br.readLine()) != null) {
            String[] fields = line.split(",");
            if (fields.length >= 2) {
                movieTitle.put(fields[0].trim(), fields[1].trim());
            }
        }
        br.close();
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        if (fields.length >= 3) {
            try {
                String userId = fields[0].trim();
                String movieId = fields[1].trim();
                float rating = Float.parseFloat(fields[2].trim());
                String gender = userGender.get(userId);
                String title = movieTitle.get(movieId);
                if (gender != null && title != null) {
                    context.write(new Text(title), new Text(gender + ":" + rating));
                }
            } catch (NumberFormatException e) {}
        }
    }
}

class GenderReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        float maleSum = 0;
        int maleCount = 0;
        float femaleSum = 0;
        int femaleCount = 0;

        for (Text val : values) {
            String[] parts = val.toString().split(":");
            if (parts.length == 2) {
                float rating = Float.parseFloat(parts[1]);
                if (parts[0].equals("M")) {
                    maleSum += rating;
                    maleCount++;
                } else if (parts[0].equals("F")) {
                    femaleSum += rating;
                    femaleCount++;
                }
            }
        }

        float maleAvg = maleCount > 0 ? maleSum / maleCount : 0;
        float femaleAvg = femaleCount > 0 ? femaleSum / femaleCount : 0;

        context.write(key, new Text("Male_Avg: " + maleAvg + ", Female_Avg: " + femaleAvg));
    }
}