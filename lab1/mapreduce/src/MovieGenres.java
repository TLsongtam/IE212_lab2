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

public class MovieGenres {

    public static class GenreMapper extends Mapper<Object, Text, Text, Text> {
        private HashMap<String, String> movieGenreMap = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            FileSystem fs = FileSystem.get(context.getConfiguration());
            BufferedReader br = new BufferedReader(
                new InputStreamReader(fs.open(new Path("/lab1/input/movies.txt"))));
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length >= 3) {
                    movieGenreMap.put(parts[0].trim(), parts[2].trim());
                }
            }
            br.close();
        }

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
                String genres = movieGenreMap.get(movieId);
                if (genres != null) {
                    for (String genre : genres.split("\\|")) {
                        context.write(new Text(genre.trim()), new Text(rating));
                    }
                }
            } catch (NumberFormatException e) {}
        }
    }

    public static class GenreReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            double sum = 0;
            int count = 0;
            for (Text val : values) {
                sum += Double.parseDouble(val.toString());
                count++;
            }
            double avg = sum / count;
            context.write(key, new Text(String.format("AverageRating: %.2f (TotalRatings: %d)", avg, count)));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Movie Genres");
        job.setJarByClass(MovieGenres.class);
        job.setMapperClass(GenreMapper.class);
        job.setReducerClass(GenreReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}