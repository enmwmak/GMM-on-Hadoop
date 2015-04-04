/*
 * Implement the EM algorithm for training GMM and save the GMM parameters to text file
 * Example usage:
 * 		cd <Workspace>/MapReduce/bin
 * 		java sequential.gmm.GMM <dimension> <No. of mixtures> <No. of iters> <data file> [output file]
 * 		java sequential.gmm.GMM 60 256 10 ../matlab/input_data.txt ../matlab/gmm.txt
 * 
 * Author: Man-Wai MAK, Dept. of EIE, The Hong Kong Polytechnic University
 * Version: 1.0
 * Date: March 2015
 * 
 * This file is subject to the terms and conditions defined in
 * file 'license.txt', which is part of this source code package.
 */

package sequential.gmm;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Scanner;

public class GMM {

	private int dim; 								// Dimension of feature vectors
	private int nMix; 								// Number of mixtures
	private double[] pi; 							// Mixture coefficients
	private double[][] mu; 							// Mean vectors mu[0..nMix-1][0..dim-1]
	private double[][] sigma; 						// Diagonal covariance Sigma[0..nMix-1][0..dim-1]
	private double constant; 						// Constant term in loglikelihood of 1 Gauss
	private double[] varFloor;						// Variance floor for each dimension
	private static final double REG_VAL = 0.0;		// Regularization parameter for GMM variance
	private static final double VAR_FLOOR_FACTOR = 0.01;	// Variance floor factor for avoiding zero variance
	
	public GMM(int dim, int nMix) {
		this.dim = dim;
		this.nMix = nMix;
		pi = new double[nMix];
		mu = new double[nMix][dim];
		sigma = new double[nMix][dim];
		constant = -(dim / 2) * Math.log(2 * Math.PI);
		varFloor = new double[dim];
	}
	
	public void init(double[][] trnData) {
		int nData = trnData.length;
		double[] var = getFeatureVariance(trnData);
		for (int j=0; j<dim; j++) {
			varFloor[j] = VAR_FLOOR_FACTOR * var[j];
		}
		int[] ridx = getRandomIndex(nData, nMix);
		for (int i = 0; i < nMix; i++) {
			for (int j = 0; j < dim; j++) {
				mu[i][j] = trnData[ridx[i]][j];
				sigma[i][j] = var[j];
			}
			pi[i] = 1.0 / (double)nMix;
		}
	}

	public void init() {
		for (int j=0; j<dim; j++) {
			varFloor[j] = VAR_FLOOR_FACTOR * varFloor[j];
		}
		Random rnd = new Random(0);					// Random number with same seed for every run
		for (int i = 0; i < nMix; i++) {
			for (int j = 0; j < dim; j++) {
				mu[i][j] = (rnd.nextDouble()-0.5)*2;
				sigma[i][j] = 1.0;
			}
			pi[i] = 1.0 / (double)nMix;
		}
	}
	
	public double[][] loadData(String datafile) {
		Scanner scanner = null;
		try {
			scanner = new Scanner(new File(datafile));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		ArrayList<double[]> list = new ArrayList<double[]>();
		while (scanner.hasNextLine()) {
			String line = scanner.nextLine();
			String[] token = line.split("\\s+|,");		// Either spacee or ',' as delimiter
			double x[] = new double[token.length];
			for (int i=0; i<x.length; i++) {
				x[i] = Double.parseDouble(token[i]);
			}
			list.add(x);
		}
		double[][] trnData = new double[list.size()][];
		for (int t=0; t<list.size(); t++) {
			trnData[t] = list.get(t);
		}
		return trnData;
	}

	public void train(double trnData[][], int nIters) {
		this.init(trnData);
		for (int iter = 1; iter <=nIters; iter++) {
			double totalLh = getTotalLogLikelihood(trnData);
			double minSigma = getMinimum(sigma);
			System.out.printf("Iter %d: Likelihood = %.2f; MinSigma = %.5f\n", iter, totalLh, minSigma);
			SuffStats suffStats = compSuffStats(trnData);
			maximize(trnData, suffStats);
		}
	}
	
	
	/*
	 * Generate k unique random numbers from 0 to n-1
	 */
	private int[] getRandomIndex(int n, int k) {
		ArrayList<Integer> list = new ArrayList<Integer>();
		for (int t=0; t<n; t++) {
			list.add(t);
		}
		Collections.shuffle(list);				// Shuffle the list
		int[] ridx = new int[k];
		for (int t=0; t<k; t++) {				// Return the first k elements of shuffled list
			ridx[t] = list.get(t);
		}
		return ridx;
	}
	
	private double[] getFeatureVariance(double[][] trnData) {
		int nData = trnData.length;
		double[] var = new double[dim];
		double[] mean = getFeatureMean(trnData);
		for (int j=0; j<dim; j++) {
			double sum = 0;
			for (int t=0; t<nData; t++) {
				double temp = trnData[t][j] - mean[j];
				sum += temp*temp;
			}
			var[j] = sum/(nData-1);
		}
		return var;
	}
	
	private double[] getFeatureMean(double[][] trnData) {
		int nData = trnData.length;
		double[] mean = new double[dim];
		for (int j=0; j<dim; j++) {
			double sum = 0;
			for (int t=0; t<nData; t++) {
				sum += trnData[t][j];
			}
			mean[j] = sum/nData;
		}
		return mean;
	}		

	/*
	 * E-step: Computation of sufficient statistics. Note that to reduce memory consumption, we
	 * compute all sufficient statistics here so that we do not need to keep an N x K posterior
	 * probability matrix. 
	 */
	private SuffStats compSuffStats(double x[][]) {
		SuffStats suffStats = new SuffStats(dim, nMix);
		int nData = x.length;
		for (int t = 0; t < nData; t++) {
			double[] post = getPosterior(x[t], pi, mu, sigma);
			for (int i = 0; i < nMix; i++) {
				suffStats.ss0[i] += post[i];
				for (int j = 0; j < dim; j++) {
					double tmp = post[i] * x[t][j];
					suffStats.ss1[i][j] += tmp;
					suffStats.ss2[i][j] += tmp * x[t][j] ;
				}
			}
		}
		return suffStats;
	}
	
	
	private double[] getPosterior(double[] xt, double[] pi, double[][] mu, double[][] sigma) {
		double[] post = new double[nMix];
		double[] likeLh = new double[nMix];
		double sum = 0.0;
		for (int i = 0; i < nMix; i++) {
			likeLh[i] = pi[i] * getComponentLikelihood(xt, mu[i], sigma[i]);
			sum += likeLh[i];
		}
		for (int i = 0; i < nMix; i++) {
			post[i] = likeLh[i] / sum;
		}
		return post;
	}

	public double getComponentLikelihood(double[] xt, double[] mui, double[] sigmai) {
		double sum1 = 0.0;
		double sum2 = 0.0;
		for (int j = 0; j < dim; j++) {
			sum1 += Math.log(sigmai[j]+REG_VAL);
			double temp = (xt[j] - mui[j]);
			sum2 += temp * temp / sigmai[j];
		}
		double llh = constant - 0.5*sum1 - 0.5 * sum2;
		return Math.exp(llh);
	}
	
	public double getLogLikelihood(double[] xt) {
		double sum = 0.0;
		for (int i = 0; i < nMix; i++) {
			sum += pi[i] * getComponentLikelihood(xt, mu[i], sigma[i]);
		}
		return(Math.log(sum));
	}

	public double getTotalLogLikelihood(double[][] x) {
		int nData = x.length;
		double totalLh = 0.0;
		for (int t = 0; t < nData; t++) {
			totalLh += getLogLikelihood(x[t]);
		}
		return totalLh;
	}

	/*
	 * Perform the M-step: Update GMM parameters based on sufficient statistics
	 */
	private void maximize(double[][] x, SuffStats suffStats) {
		int nData = x.length;
		for (int i = 0; i < nMix; i++) {
			pi[i] = suffStats.ss0[i] / nData;
			for (int j = 0; j < dim; j++) {
				mu[i][j] = suffStats.ss1[i][j]/suffStats.ss0[i];
				sigma[i][j] = suffStats.ss2[i][j]/suffStats.ss0[i] - mu[i][j]*mu[i][j] + REG_VAL;
				if (sigma[i][j] < varFloor[j]) {
					System.out.printf("Warning: sigma[%d][%d] set to variance floor %.5f\n",i,j,varFloor[j]);
					sigma[i][j] = varFloor[j];
				}
			}
		}
	}


	
	private double getMinimum(double[][] x) {
		double min = Double.MAX_VALUE;
		for (int i=0; i<x.length; i++) {
			for (int j=0; j<x[i].length; j++) {
				if (x[i][j] < min) {
					min = x[i][j];
				}
			}
		}
		return min;
	}
	
	/*
	 * Sufficient statistics are the basic statistics needed to be estimated to compute the 
	 * desired parameters. For a GMM mixture, these are the count, and the first and 
	 * second moments required to compute the mixture weight, mean and variance
	 */
	class SuffStats {
		double[] ss0; // 0th-order sufficient statistics (sum_t gamma_t)
		double[][] ss1; // 1st-order sufficient statistics (sum_t gamma_t x_t)
		double[][] ss2; // 2nd-order sufficient statistics (sum_t gamma_t x_t x_t')

		public SuffStats(int dim, int nMix) {
			ss0 = new double[nMix];
			ss1 = new double[nMix][dim];
			ss2 = new double[nMix][dim];
		}
	}

	@SuppressWarnings("unused")
	private void printData(double[][] trnData) {
		for (double[] vector : trnData) {
			for (double x : vector) {
				System.out.printf("%.3f ",x);				
			}
			System.out.println();
		}
	}
	
	public void printModel() {
		System.out.println("Means:");
		for (int i=0; i<nMix; i++) {
			for (int j=0; j<dim; j++) {
				System.out.printf("%.3f  ", mu[i][j]);
			}
			System.out.println();
		}
		System.out.println("Variances:");
		for (int i=0; i<nMix; i++) {
			for (int j=0; j<dim; j++) {
				System.out.printf("%.3f  ", sigma[i][j]);
			}
			System.out.println();
		}		
		System.out.println("Mixture Coefficients:");
		for (int i=0; i<nMix; i++) {
			System.out.printf("%.3f  ",pi[i]);
		}
		System.out.println();		
	}
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		for (int i=0; i<nMix; i++) {
			sb.append(String.format("%.5f ", pi[i]));
		}
		sb.append("\n");
		for (int i=0; i<nMix; i++) {
			for (int j=0; j<dim; j++) {
				sb.append(String.format("%.5f ", mu[i][j]));
			}
			sb.append("\n");
		}
		for (int i=0; i<nMix; i++) {
			for (int j = 0; j<dim; j++) {
				sb.append(String.format("%.5f ", sigma[i][j]));
			}
			sb.append("\n");
		}
		return(sb.toString());
	}

	/*
	 * Load GMM parameters. 
	 */
	public void loadParameters(String gmmFile) {
		try {
			Scanner scanner = new Scanner(new File(gmmFile));
			try {
				String line = scanner.nextLine();
				String[] token = line.split(" ");
				for (int i = 0; i < nMix; i++) {
					pi[i] = Double.parseDouble(token[i]);
				}
				for (int i = 0; i < nMix; i++) {
					line = scanner.nextLine();
					token = line.split(" ");
					for (int j = 0; j < dim; j++) {
						mu[i][j] = Double.parseDouble(token[j]);
					}
				}
				for (int i = 0; i < nMix; i++) {
					line = scanner.nextLine();
					token = line.split(" ");
					for (int j = 0; j < dim; j++) {
						sigma[i][j] = Double.parseDouble(token[j]);
					}
				}
			} finally {
				scanner.close();
			}
		} catch (NoSuchElementException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	
	/*
	 * Save GMM parameters to file
	 */
	public void saveParameters(String gmmFile) {
		try {
			PrintWriter out = new PrintWriter(new File(gmmFile));
			try {
				out.println(this.toString());
			} finally {
				out.close();
			}
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		}
	}
	
	
	public static void main(String[] args) {
		if (args.length < 4 || args.length > 5) {
			System.out.println("Usage: java sequential.gmm.GMM <dimension> <No. of mixtures> <No. of iters> <data file> [output file]");
			System.out.println("Example: java sequential.gmm.GMM 60 32 20 ../matlab/2D_data.txt ../matlab/gmm.txt");
			return;
		}
		int dim = Integer.parseInt(args[0]);
		int nMix = Integer.parseInt(args[1]);
		int nIters = Integer.parseInt(args[2]);
		String dataFile = args[3];
		GMM gmm = new GMM(dim, nMix);
		double[][] trnData = gmm.loadData(dataFile);
		gmm.train(trnData, nIters);
		if (args.length == 5) {
			String paraFile = args[4];
			System.out.println("Saving parameter file " + paraFile);
			gmm.saveParameters(paraFile);
		} else {
			gmm.printModel();
		}
	}
}
