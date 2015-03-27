package parallel.gmm;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import cpu.CPU;

/*
 * MapReduce class for computing the global mean vectors of input files containing
 * one row per vector. For each mapper, a combiner is used for accumulating the partial sum
 * of that mapper.
 */
public class MapRedOneMean {
	
	/*
	 * Emit one <1,vector[]> for each line of input files. The last element of vector[]
	 * is 1.0 to inform the combiner that each vector is unique.
	 */
	public static class OneMeanMapper extends Mapper<LongWritable, Text, IntWritable, DoubleArrayWritable> {
		private final static IntWritable keyOut = new IntWritable(1);		// Intermediate key emitted by the map task
		private DoubleArrayWritable valueOut = new DoubleArrayWritable();
		
		public void map(LongWritable key, Text value, Context context)
							throws IOException, InterruptedException {
			String[] token = value.toString().split("\\s+|,");
			int dim = token.length-1;								// token[0] is the ID, not part of the vector
			DoubleWritable[] vector = new DoubleWritable[dim+1];	// Last element is the count, should always be 1 in the mapper	
			for (int i=0; i<dim; i++) {
				vector[i] = new DoubleWritable();
				vector[i].set(Double.parseDouble(token[i+1]));
			}
			vector[dim] = new DoubleWritable();
			vector[dim].set(1.0);
			valueOut.set(vector);
			context.write(keyOut, valueOut);
		}
	}

	/*
	 * For each mapper, emit <key,vector> pair, where the vector contains the partial sum 
	 * vector of this mapper and the key is the same as the key received by this combiner. 
	 */
	public static class OneMeanCombiner extends Reducer<IntWritable, DoubleArrayWritable, IntWritable, DoubleArrayWritable> {

		public void reduce(IntWritable key, Iterable<DoubleArrayWritable> values, Context context) 
							throws IOException, InterruptedException {

			Iterator<DoubleArrayWritable> iter = values.iterator();
			int dim = 1;
			DoubleWritable[] sum = null;
			int numVectors = 0;
			while (iter.hasNext()) {				
				numVectors++;
				Writable[] vector = iter.next().get();				// vector[] is of dimension dim+1
				if (sum == null) {									// First vector, init sum[]
					dim = vector.length-1;							// Last entry contains numVectors
					sum = new DoubleWritable[dim+1];				// Last entry contains numVectors
					for (int i=0; i<sum.length; i++) {
						sum[i] = new DoubleWritable(0.0);
					}
				}
				for (int i=0; i<dim; i++) {
					DoubleWritable x = (DoubleWritable)vector[i];
					sum[i].set(sum[i].get() + x.get());				// Accumulate vector
				}
				CPU.wasteCpuTime(CPU.NUM_UNIT);						// For ease of time measurement
			}
			sum[dim].set(numVectors);								// No. of vecs summed in this combiner
			DoubleArrayWritable valueOut = new DoubleArrayWritable();
			valueOut.set(sum);			
			context.write(key, valueOut);
		}
	}
	
	/*
	 * For each key-value pair from the combiner, sum the partial count. Divide the total sum by the number
	 * of vectors accumulated.
	 */
	public static class OneMeanReducer extends Reducer<IntWritable, DoubleArrayWritable, IntWritable, Text> {

		public void reduce(IntWritable key, Iterable<DoubleArrayWritable> values,
				Context context) throws IOException, InterruptedException {

			Iterator<DoubleArrayWritable> iter = values.iterator();
			DoubleWritable[] sum = null;
			int dim = 1;
			int numVectors = 0;
			while (iter.hasNext()) {
				Writable[] vector = iter.next().get();
				if (sum == null) {								// First vector, init array sum[]
					dim = vector.length-1;						// The last entry contains the numVectors processed by the combiner or mapper
					sum = new DoubleWritable[dim];
					for (int i=0; i<dim; i++) {
						sum[i] = new DoubleWritable(0.0);		// Need to create object for each entry
					}
				}
				DoubleWritable x = (DoubleWritable)vector[dim];
				int numAccVectors = (int)x.get();				// Get the number of accumulated vectors in mapper (=1) or combiner
				numVectors += numAccVectors;
				for (int i=0; i<dim; i++) {
					x = (DoubleWritable)vector[i];
					sum[i].set(sum[i].get() + x.get());			// Accumulate vector
				}
				CPU.wasteCpuTime(CPU.NUM_UNIT);
			}
						
			StringBuilder sb = new StringBuilder();
			for (int i=0; i<dim; i++) {
				sb.append(String.format("%.2f ", sum[i].get()/numVectors));
			}
			Text valueOut = new Text();
			valueOut.set(sb.toString());
			context.write(key, valueOut);
		}
	}
	
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "onemean");
		job.setJarByClass(MapRedOneMean.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(DoubleArrayWritable.class);

		job.setMapperClass(OneMeanMapper.class);
		job.setReducerClass(OneMeanReducer.class);
		job.setCombinerClass(OneMeanCombiner.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
		
	
}


