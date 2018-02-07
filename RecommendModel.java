import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;
import java.util.*;

class Pair {
    String key;
    double value;

    Pair(String key, double value) {
        this.key = key;
        this.value = value;
    }
}

public class RecommendModel {
    public static class RatingMapper extends Mapper<LongWritable, Text, Text, Text> {

        // map method
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //input: userId:movieId \t rating
            //outputKey: userId
            //outputValue: movieId:rating
            String line = value.toString().trim();
            String[] userIdMovieId_rating = line.split("\t");
            String rating = userIdMovieId_rating[1];
            String[] userId_movieId = userIdMovieId_rating[0].split(":");
            String userId = userId_movieId[0];
            String movieId = userId_movieId[1];
            context.write(new Text(userId), new Text(movieId + ":" + rating));
        }
    }

    public static class FlagMapper extends Mapper<LongWritable, Text, Text, Text> {

        // map method
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //input: userId,movieId,rating
            //outputKey: userId
            //outputValue: movieId=true, means this movie was rated by user
            String line = value.toString().trim();
            String[] user_movie_rating = line.split(",");
            String userId = user_movie_rating[0];
            String movieId = user_movie_rating[1];
            context.write(new Text(userId), new Text(movieId + "=true"));
        }
    }

    public static class RecommenderReducer extends Reducer<Text, Text, DBOutputWritable, NullWritable> {
        private PriorityQueue<Pair> queue = null;
        private int k;

        private Comparator<Pair> pairComparator = new Comparator<Pair>() {
            public int compare(Pair left, Pair right) {
                if (Double.compare(left.value, right.value) != 0) {
                    return Double.compare(left.value, right.value);
                }
                return right.key.compareTo(left.key);
            }
        };

        // get the k parameter from the configuration: top k
        @Override
        public void setup(Context context) {
            Configuration conf = context.getConfiguration();
            this.k = conf.getInt("k", 5);
            this.queue = new PriorityQueue<Pair>(k, pairComparator);
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            //rank top k movies then write to database
			/* key: userId
			   values: <movieId1:rating, movieId2:rating, ..., movieId1=true, movieIdK=true,...>
			 */

            Map<String, Double> ratingMap = new HashMap<String, Double>();
            Set<String> ratedMovies = new HashSet<String>();

            for (Text value: values) {
                if (value.toString().contains(":")) {
                    //movieId:rating
                    String[] movieId_rating = value.toString().trim().split(":");
                    ratingMap.put(movieId_rating[0], Double.parseDouble(movieId_rating[1]));
                } else {
                    //movieId=true
                    String movieId = value.toString().trim().split("=")[0];
                    ratedMovies.add(movieId);
                }
            }

            for (Map.Entry<String, Double> entry: ratingMap.entrySet()) {
                Pair pair = new Pair(entry.getKey(), entry.getValue());
                if (ratedMovies.contains(pair.key)) {
                    continue;
                }
                if (queue.size() < k) {
                    queue.add(pair);
                } else {
                    Pair peak = queue.peek();
                    if (pairComparator.compare(pair, peak) > 0) {
                        queue.poll();
                        queue.add(pair);
                    }
                }
            }

            List<Pair> pairs = new ArrayList<Pair>();
            while (!queue.isEmpty()) {
                pairs.add(queue.poll());
            }

            int size = pairs.size();
            for (int i = size - 1; i >= 0; i--) {
                Pair pair = pairs.get(i);
                context.write(new DBOutputWritable(Integer.parseInt(key.toString()), Integer.parseInt(pair.key), pair.value), NullWritable.get());
            }
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("k", args[2]);

        DBConfiguration.configureDB(conf,
                "com.mysql.jdbc.Driver",
                "jdbc:mysql://ip_address:port/test", // input your ip address and port
                "root",
                "root");

        Job job = Job.getInstance(conf);
        job.setJarByClass(RecommendModel.class);

        job.setMapperClass(RecommendModel.RatingMapper.class);
        job.setMapperClass(RecommendModel.FlagMapper.class);
        job.setReducerClass(RecommendModel.RecommenderReducer.class);

        job.addArchiveToClassPath(new Path("/mysql/mysql-connector-java-5.1.39-bin.jar"));

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(DBOutputWritable.class);
        job.setOutputValueClass(NullWritable.class);

        job.setOutputFormatClass(DBOutputFormat.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, RecommendModel.RatingMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RecommendModel.FlagMapper.class);

        DBOutputFormat.setOutput(job, "recommender",
                new String[] {"user_id", "movie_id", "rating"});

        job.waitForCompletion(true);
    }
}
