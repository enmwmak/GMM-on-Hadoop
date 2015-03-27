package parallel.wordcount;

import java.io.IOException;
import java.util.HashMap;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/*
 * Word count example with in-mapping combiners.
 */
public class WordCountInMapperComb {
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		private Text word = new Text();

		/*
		 * Implement in-mapper combiner by using a hashmap that store the partial
		 * count of the words encountered by this mapper. The in-mapper combiner
		 * can reduce the number of key-value pairs emitted by the mapper if there
		 * are repeated words within a line in the input Text object (value).
		 */
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			HashMap<String, Integer> wordFreq = new HashMap<String, Integer>();
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line, "\\s+|,");
			while (tokenizer.hasMoreTokens()) {
				String token = tokenizer.nextToken();
				if (wordFreq.containsKey(token)) {
					int count = wordFreq.get(token) + 1;	// Increment token count by 1
					wordFreq.put(token, count);
				} else {
					wordFreq.put(token, 1);
				}
			}
			
			// Loop through the hashmap and emit <token,count> pair
			Set<String> words = wordFreq.keySet();
			for (String w : words) {
				int total = wordFreq.get(w);
				word.set(w);
				context.write(word, new IntWritable(total));
			}
		}
	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "wordcount");
		job.setJarByClass(WordCount.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}
