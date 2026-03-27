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

public class bai1 {
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Movie Ratings");
        job.setJarByClass(bai1.class);

        job.setMapperClass(RatingMapper.class);
        job.setReducerClass(RatingReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(FloatWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

class RatingMapper extends Mapper<LongWritable, Text, IntWritable, FloatWritable> {
    private IntWritable movieId = new IntWritable();
    private FloatWritable rating = new FloatWritable();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        if (fields.length >= 3) {
            try {
                int mId = Integer.parseInt(fields[1].trim());
                float rate = Float.parseFloat(fields[2].trim());
                movieId.set(mId);
                rating.set(rate);
                context.write(movieId, rating);
            } catch (NumberFormatException e) {
            }
        }
    }
}

class RatingReducer extends Reducer<IntWritable, FloatWritable, Text, Text> {
    private Map<Integer, String> movieTitles = new HashMap<>();
    private String maxMovie = "";
    private float maxRating = 0.0f;

    @Override
    protected void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        FileSystem fs = FileSystem.get(conf);
        Path moviesPath = new Path("/input/movies.txt");

        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(moviesPath)));
        String line;
        while ((line = br.readLine()) != null) {
            String[] fields = line.split(",");
            if (fields.length >= 2) {
                try {
                    int id = Integer.parseInt(fields[0].trim());
                    String title = fields[1].trim();
                    movieTitles.put(id, title);
                } catch (NumberFormatException e) {
                }
            }
        }
        br.close();
    }

    @Override
    public void reduce(IntWritable key, Iterable<FloatWritable> values, Context context)
            throws IOException, InterruptedException {
        int count = 0;
        float sum = 0;
        for (FloatWritable val : values) {
            sum += val.get();
            count++;
        }
        if (count == 0) return;

        float avg = sum / count;
        String title = movieTitles.getOrDefault(key.get(), "Unknown Movie");

        context.write(new Text(title),
                new Text("AverageRating: " + avg + " (TotalRatings: " + count + ")"));

        if (count >= 5 && avg > maxRating) {
            maxRating = avg;
            maxMovie = title;
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        if (!maxMovie.isEmpty()) {
            context.write(new Text(maxMovie),
                    new Text("is the highest rated movie with an average rating of " + maxRating + " among movies with at least 5 ratings."));
        } else {
            context.write(new Text("No movie found"),
                    new Text("No movie has at least 5 ratings."));
        }
}
}