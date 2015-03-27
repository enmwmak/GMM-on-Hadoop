/*
 * Store the sufficient statistics of a GMM. Serializer and de-serializer are provided for
 * Hadoop to pass the objects of this class between mappers, combinners, and reducers
 * 
 * Author: Man-Wai MAK, Dept. of EIE, The Hong Kong Polytechnic University
 * Version: 1.0
 * Date: March 2015
 * 
 * This file is subject to the terms and conditions defined in
 * file 'license.txt', which is part of this source code package.
*/

package parallel.gmm;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/*
 * Sufficient statistics are the basic statistics needed to be estimated to compute the 
 * desired parameters. For a GMM mixture, these are the count, and the first and 
 * second moments required to compute the mixture weight, mean and variance
 * While the likelihood is not a sufficient statistic, it is included here for ease of
 * debugging.
 */
class SuffStats implements Writable {
	private final static int NUM_MIX = Config.NUM_MIX;
	private final static int DIM = Config.DIM;		

	private double[] ss0; 	// 0th-order sufficient statistics
	private double[][] ss1; // 1st-order sufficient statistics
	private double[][] ss2; // 2nd-order sufficient statistics
	private double likelh;	// Likelihood

	/*
	 * Note: All Writable implementations must have a default constructor so that the MapReduce 
	 * framework can instantiate them, and populate their fields by calling readFields().
	 * https://www.safaribooksonline.com/library/view/hadoop-the-definitive/9781449328917/ch04.html.
	 */	
	public SuffStats() {
		ss0 = new double[NUM_MIX];
		ss1 = new double[NUM_MIX][DIM];
		ss2 = new double[NUM_MIX][DIM];
		likelh = 0.0;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		writeDoubleArray(out, ss0);
		for (double[] dArray : ss1) {
			writeDoubleArray(out, dArray);
		}
		for (double[] dArray : ss2) {
			writeDoubleArray(out, dArray);
		}
		out.writeDouble(likelh);
	}
	
	private void writeDoubleArray(DataOutput out, double[] dArray) throws IOException {
		for (double d : dArray) {
			out.writeDouble(d);
		}
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		readDoubleArray(in, ss0);
		for (double[] dArray : ss1) {
			readDoubleArray(in, dArray);
		}
		for (double[] dArray : ss2) {
			readDoubleArray(in, dArray);
		}
		likelh = in.readDouble();
	}		
	
	private void readDoubleArray(DataInput in, double[] dArray) throws IOException {
		for (int i=0; i<dArray.length; i++) {
			dArray[i] = in.readDouble();
		}
	}
	
	public void accumulate(double[] gamma, double[] xt, double[][] mu) {
		for (int i = 0; i < gamma.length; i++) {
			ss0[i] += gamma[i];
			for (int j = 0; j < xt.length; j++) {
				double temp = gamma[i] * xt[j];
				ss1[i][j] += temp;
				ss2[i][j] += temp * xt[j];
			}
		}
	}
	
	public void accumulate(SuffStats curSuffStats) {
		for (int i = 0; i < curSuffStats.ss0.length; i++) {
			ss0[i] += curSuffStats.ss0[i];
			for (int j = 0; j < curSuffStats.ss1[i].length; j++) {
				ss1[i][j] += curSuffStats.ss1[i][j];
				ss2[i][j] += curSuffStats.ss2[i][j];
			}
		}
		likelh += curSuffStats.likelh;
	}

	public double[] getSs0() {
		return ss0;
	}

	public double[][] getSs1() {
		return ss1;
	}

	public double[][] getSs2() {
		return ss2;
	}
			
	public double getLikelh() {
		return likelh;
	}

	public void setLikelh(double likelh) {
		this.likelh = likelh;
	}

	@SuppressWarnings("unused")
	private void printDoubleArray(double[] dArray) {
		System.out.println("ss0:");
		for (double d : dArray) {
			System.out.printf("%.2f ", d);
		}
		System.out.println();
	}
		
}