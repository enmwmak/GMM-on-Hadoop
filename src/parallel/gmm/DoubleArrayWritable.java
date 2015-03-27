package parallel.gmm;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;

public class DoubleArrayWritable extends ArrayWritable {
	public DoubleArrayWritable() {
		super(DoubleWritable.class);
	}
}
