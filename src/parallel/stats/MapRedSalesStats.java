package parallel.stats;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MapRedSalesStats {
	
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		private Text custId = new Text();
		private Text custStats = new Text();
		
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] token = value.toString().split(",");
			int numEntries = token.length-1;					// token[0] is the custId
			double[] custSales = new double[numEntries];		
			for (int i=0; i<numEntries; i++) {
				custSales[i] = Double.parseDouble(token[i+1]);
			}
			Statistics stats = new Statistics(custSales);
			double mean = stats.getMean();
			double var = stats.getVariance();
			StringBuilder sb = new StringBuilder()
				.append(String.format("%.1f", mean) + ", ")
				.append(String.format("%.1f", var));
			custId.set(token[0]);
			custStats.set(sb.toString());
			context.write(custId, custStats);
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "salesstats");
		job.setJarByClass(MapRedSalesStats.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Map.class);
		
		job.setNumReduceTasks(0);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}
