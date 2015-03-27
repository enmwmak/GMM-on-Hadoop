package parallel.stats;

public class Statistics {
	double[] data;
	double size;
	public Statistics(double data[]) {
		this.data = data;
		this.size = data.length;
	}

	double getMean() {
		double sum = 0.0;
		for (double a : data) {
			for (int i=0; i<100000; i++) {	
				//Math.exp(1.0);			// Dummy calculation lengthen computation
			}
			sum += a;
		}
		return sum / size;
	}

	double getVariance() {
		double mean = getMean();
		double temp = 0;
		for (double a : data) {
			temp += (mean - a) * (mean - a);
		}
		return temp / size;
	}
}


