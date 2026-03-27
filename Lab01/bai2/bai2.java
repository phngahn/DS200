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

public class bai2 {
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.exit(-1);
        }
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Genre Ratings");
        job.setJarByClass(bai2.class);
        job.setMapperClass(GenreMapper.class);
        job.setReducerClass(GenreReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

class GenreMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
    private Map<Integer, String[]> movieGenres = new HashMap<>();
    private Text genreKey = new Text();
    private FloatWritable ratingVal = new FloatWritable();

    @Override
    protected void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        FileSystem fs = FileSystem.get(conf);
        Path moviesPath = new Path("/input/movies.txt");
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(moviesPath)));
        String line;
        while ((line = br.readLine()) != null) {
            String[] fields = line.split(",");
            if (fields.length >= 3) {
                try {
                    int id = Integer.parseInt(fields[0].trim());
                    String genres = fields[2].trim();
                    movieGenres.put(id, genres.split("\\|"));
                } catch (NumberFormatException e) {}
            }
        }
        br.close();
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        if (fields.length >= 3) {
            try {
                int movieId = Integer.parseInt(fields[1].trim());
                float rate = Float.parseFloat(fields[2].trim());
                ratingVal.set(rate);
                String[] genres = movieGenres.get(movieId);
                if (genres != null) {
                    for (String g : genres) {
                        genreKey.set(g.trim());
                        context.write(genreKey, ratingVal);
                    }
                }
            } catch (NumberFormatException e) {}
        }
    }
}

class GenreReducer extends Reducer<Text, FloatWritable, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<FloatWritable> values, Context context)
            throws IOException, InterruptedException {
        int count = 0;
        float sum = 0;
        for (FloatWritable val : values) {
            sum += val.get();
            count++;
        }
        if (count == 0) return;
        float avg = sum / count;
        context.write(key, new Text("AverageRating: " + avg + " (TotalRatings: " + count + ")"));
    }
}