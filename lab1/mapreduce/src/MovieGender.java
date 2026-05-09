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


public class MovieGender {

    public static class GenderMapper extends Mapper<Object, Text, Text, Text> {
        private HashMap<String, String> userGenderMap = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            FileSystem fs = FileSystem.get(context.getConfiguration());
            BufferedReader br = new BufferedReader(
                new InputStreamReader(fs.open(new Path("/lab1/input/users.txt"))));
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length >= 2) {
                    userGenderMap.put(parts[0].trim(), parts[1].trim());
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
            String userId = parts[0].trim();
            String movieId = parts[1].trim();
            String rating = parts[2].trim();
            try {
                Double.parseDouble(rating);
                String gender = userGenderMap.get(userId);
                if (gender != null) {
                    context.write(new Text(movieId), new Text(gender + ":" + rating));
                }
            } catch (NumberFormatException e) {}
        }
    }

    public static class GenderReducer extends Reducer<Text, Text, Text, Text> {
        private HashMap<String, String> movieMap = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            FileSystem fs = FileSystem.get(context.getConfiguration());
            BufferedReader br = new BufferedReader(
                new InputStreamReader(fs.open(new Path("/lab1/input/movies.txt"))));
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
            double maleSum = 0, femaleSum = 0;
            int maleCount = 0, femaleCount = 0;

            for (Text val : values) {
                String[] parts = val.toString().split(":");
                if (parts.length < 2) continue;
                String gender = parts[0].trim();
                double rating = Double.parseDouble(parts[1].trim());
                if (gender.equals("M")) {
                    maleSum += rating;
                    maleCount++;
                } else if (gender.equals("F")) {
                    femaleSum += rating;
                    femaleCount++;
                }
            }

            String maleAvg = maleCount > 0 ? String.format("%.2f", maleSum / maleCount) : "N/A";
            String femaleAvg = femaleCount > 0 ? String.format("%.2f", femaleSum / femaleCount) : "N/A";

            String title = movieMap.getOrDefault(key.toString(), key.toString());
            context.write(new Text(title), new Text("Male: " + maleAvg + ", Female: " + femaleAvg));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Movie Gender");
        job.setJarByClass(MovieGender.class);
        job.setMapperClass(GenderMapper.class);
        job.setReducerClass(GenderReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
