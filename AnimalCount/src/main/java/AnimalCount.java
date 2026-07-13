import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AnimalCount {

    // 1. MAPPER CLASS
    public static class AnimalMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text animalName = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Read line, trim trailing spaces, and standardize casing
            String line = value.toString().trim();
            if (!line.isEmpty()) {
                animalName.set(line);
                // Emits a key-value pair: (AnimalName, 1)
                context.write(animalName, one);
            }
        }
    }

    // 2. REDUCER CLASS
    public static class AnimalReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            // Aggregate the counts for each animal key
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            // Emits the final key-value pair: (AnimalName, Total Count)
            context.write(key, result);
        }
    }

    // 3. DRIVER METHOD
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: AnimalCount <input path> <output path>");
            System.exit(-1);
        }
        
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Animal Sighting Census");
        
        job.setJarByClass(AnimalCount.class);
        job.setMapperClass(AnimalMapper.class);
        job.setCombinerClass(AnimalReducer.class); // Optimization step
        job.setReducerClass(AnimalReducer.class);
        
        // Define output key and value data types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
