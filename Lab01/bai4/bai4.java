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

public class bai4 {
    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.exit(-1);
        }
        Configuration conf = new Configuration();
        conf.set("users.path", args[2]);
        Job job = Job.getInstance(conf, "AgeGroup Ratings");
        job.setJarByClass(bai4.class);
        job.setMapperClass(AgeGroupMapper.class);
        job.setReducerClass(AgeGroupReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[3]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

class AgeGroupMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Map<String, String> userAgeGroup = new HashMap<>();
    private Map<String, String> movieTitle = new HashMap<>();

    private String getAgeGroup(int age) {
        if (age <= 18) return "0-18";
        else if (age <= 35) return "18-35";
        else if (age <= 50) return "35-50";
        else return "50+";
    }

    @Override
    protected void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        FileSystem fs = FileSystem.get(conf);

        Path usersPath = new Path(conf.get("users.path"));
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(usersPath)));
        String line;
        while ((line = br.readLine()) != null) {
            String[] fields = line.split(",");
            if (fields.length >= 3) {
                try {
                    String userId = fields[0].trim();
                    int age = Integer.parseInt(fields[2].trim());
                    userAgeGroup.put(userId, getAgeGroup(age));
                } catch (NumberFormatException e) {}
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
                String ageGroup = userAgeGroup.get(userId);
                String title = movieTitle.get(movieId);
                if (ageGroup != null && title != null) {
                    context.write(new Text(title), new Text(ageGroup + ":" + rating));
                }
            } catch (NumberFormatException e) {}
        }
    }
}

class AgeGroupReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        Map<String, Float> sumMap = new LinkedHashMap<>();
        Map<String, Integer> countMap = new LinkedHashMap<>();

        String[] groups = {"0-18", "18-35", "35-50", "50+"};
        for (String g : groups) {
            sumMap.put(g, 0f);
            countMap.put(g, 0);
        }

        for (Text val : values) {
            String[] parts = val.toString().split(":");
            if (parts.length == 2) {
                String group = parts[0];
                float rating = Float.parseFloat(parts[1]);
                if (sumMap.containsKey(group)) {
                    sumMap.put(group, sumMap.get(group) + rating);
                    countMap.put(group, countMap.get(group) + 1);
                }
            }
        }

        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < groups.length; i++) {
            String g = groups[i];
            int count = countMap.get(g);
            String avg = count > 0 ? String.valueOf(sumMap.get(g) / count) : "NA";
            sb.append(g).append(": ").append(avg);
            if (i < groups.length - 1) sb.append(", ");
        }
        sb.append("]");

        context.write(key, new Text(sb.toString()));
    }
}