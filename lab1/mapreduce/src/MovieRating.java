import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MovieRating {

    public static class RatingMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;
            String[] parts = line.split(",");
            if (parts.length < 3) return;
            String movieId = parts[1].trim();
            String rating = parts[2].trim();
            try {
                Double.parseDouble(rating);
                context.write(new Text(movieId), new Text(rating));
            } catch (NumberFormatException e) {
                // bỏ qua dòng không phải rating
            }
        }
    }

    public static class RatingReducer extends Reducer<Text, Text, Text, Text> {
        private HashMap<String, String> movieMap = new HashMap<>();
        private String maxMovie = "";
        private double maxRating = 0;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            FileSystem fs = FileSystem.get(conf);
            Path moviesPath = new Path("/lab1/input/movies.txt");
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(moviesPath)));
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length >= 2) {
                    movieMap.put(parts[0].trim(), parts[1].trim());
                }
            }
            br.close();
        }

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            double sum = 0;
            int count = 0;
            for (Text val : values) {
                sum += Double.parseDouble(val.toString());
                count++;
            }
            double avg = sum / count;
            String movieId = key.toString();
            String title = movieMap.getOrDefault(movieId, movieId);

            if (count >= 2 && avg > maxRating) {
                maxRating = avg;
                maxMovie = title;
            }

            String result = String.format("AverageRating: %.2f (TotalRatings: %d)", avg, count);
            context.write(new Text(title), new Text(result));
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            if (!maxMovie.isEmpty()) {
                context.write(new Text(maxMovie),
                    new Text(String.format(
                        "is the highest rated movie with an average rating of %.2f among movies with at least 2 ratings.",
                        maxRating)));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Movie Rating");
        job.setJarByClass(MovieRating.class);
        job.setMapperClass(RatingMapper.class);
        job.setReducerClass(RatingReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}