package parallel.stats;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class MapRedMaxSales {
	/*
	 * Emit <1,partialMax> for each line in the split
	 */
	public static class Map extends Mapper<LongWritable, Text, IntWritable, DoubleWritable> {
		private IntWritable outKey = new IntWritable(1);
		private DoubleWritable partialMax = new DoubleWritable(0.0);
		
		public void map(LongWritable key, Text value, Context context)
					throws IOException, InterruptedException {
			String[] token = value.toString().split(",");
			int numEntries = token.length-1;					// token[0] is the custId
			double max = 0.0;									// min sales is 0
			for (int i=0; i<numEntries; i++) {
				double sales = Double.parseDouble(token[i+1]);
				if (sales > max) {
					max = sales;
				}
			}
			partialMax.set(max);
			context.write(outKey, partialMax);
		}
	}

	/*
	 * Emit <1,partialMax>, where partialMax is the max of the partialMax's in the mappers in this node
	 */
	public static class Combiner extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {
		public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context)
					throws IOException, InterruptedException {
			double max = 0.0;
			for (DoubleWritable dw : values) {
				double sales = dw.get();
				if (sales > max) {
					max = sales;
				}
			}
			context.write(key, new DoubleWritable(max));
		}
	}
	
	/*
	 * Emit <globalMax> to HFDS. Note that there is only one reducer.
	 */
	public static class Reduce extends Reducer<IntWritable, DoubleWritable, NullWritable, Text> {
		public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context) 
					throws IOException, InterruptedException {
			double max = 0.0;
			for (DoubleWritable dw : values) {
				double sales = dw.get();
				if (sales > max) {
					max = sales;
				}			
			}
			Text maxSales = new Text("Maximum Sales = " + String.format("%.2f",max));
			context.write(NullWritable.get(), maxSales);
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "maxsales");
		job.setJarByClass(MapRedMaxSales.class);		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setMapperClass(Map.class);
		job.setCombinerClass(Combiner.class);
		job.setReducerClass(Reduce.class);	
		job.setNumReduceTasks(1);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
	}
}
