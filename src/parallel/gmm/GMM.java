/*
 * Author: Man-Wai MAK, Dept. of EIE, The Hong Kong Polytechnic University
 * Version: 1.0
 * Date: March 2015
 * 
 * This file is subject to the terms and conditions defined in
 * file 'license.txt', which is part of this source code package.
*/


package parallel.gmm;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class GMM {
	private int dim; 				// Dimension of feature vectors
	private int nMix; 				// Number of mixtures
	private double[] pi; 			// Mixture coefficients
	private double[][] mu; 			// Mean vectors mu[0..nMix-1][0..dim-1]
	private double[][] sigma; 		// Diagonal covariance sigma[0..nMix-1][0..dim-1]
	private double constant; 		// Constant term in loglikelihood of 1 Gauss

	/*
	 * Create a GMM object and initialize its parameters
	 */
	public GMM(int dim, int nMix) {
		this.dim = dim;
		this.nMix = nMix;
		pi = new double[nMix];
		mu = new double[nMix][dim];
		sigma = new double[nMix][dim];
		constant = -(dim / 2) * Math.log(2 * Math.PI);
		init();
	}

	/*
	 * Create a GMM object and load its parameters from file. If file not exists, initialize the
	 * GMM parameters to their default value.
	 */
	public GMM(int dim, int nMix, String gmmFile) {
		this.dim = dim;
		this.nMix = nMix;
		pi = new double[nMix];
		mu = new double[nMix][dim];
		sigma = new double[nMix][dim];
		constant = -(dim / 2) * Math.log(2 * Math.PI);
		try {
			loadParameters(gmmFile);
		} catch (IOException e) {
			System.out.println("GMM file " + gmmFile + " not found");
			init();
		}
	}
		
	public void init() {
		Random rnd = new Random(0);					// Random number with same seed for every run
		for (int i = 0; i < nMix; i++) {
			for (int j = 0; j < dim; j++) {
				mu[i][j] = (rnd.nextDouble()-0.5)*2;
				sigma[i][j] = 5.0;
			}
			pi[i] = 1.0 / (double)nMix;
		}
	}
	
	/*
	 * Load GMM parameters. If file not exists, call init() to initialize the parameters
	 */
	public void loadParameters(String gmmFile) throws IOException {
		Path pt = new Path(gmmFile);
		FileSystem fs;
		fs = FileSystem.get(new Configuration());
		BufferedReader br = new BufferedReader(new InputStreamReader(
				fs.open(pt)));
		String line = br.readLine();
		String[] token = line.split(" ");
		for (int i = 0; i < nMix; i++) {
			pi[i] = Double.parseDouble(token[i]);
		}
		for (int i = 0; i < nMix; i++) {
			line = br.readLine();
			token = line.split(" ");
			for (int j = 0; j < dim; j++) {
				mu[i][j] = Double.parseDouble(token[j]);
			}
		}
		for (int i = 0; i < nMix; i++) {
			line = br.readLine();
			token = line.split(" ");
			for (int j = 0; j < dim; j++) {
				sigma[i][j] = Double.parseDouble(token[j]);
			}
		}
		br.close();
	}
	
	public void saveParameters(String gmmFile) {
		Path pt = new Path(gmmFile);
		FileSystem fs;
		try {
			fs = FileSystem.get(new Configuration());
			BufferedWriter br = new BufferedWriter(new OutputStreamWriter(fs.create(pt, true)));
			br.write(this.toString());
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public double[] getPosterior(double[] xt) {
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
			sum1 += Math.log(sigmai[j]);
			double temp = (xt[j] - mui[j]);
			sum2 += (temp * temp)/sigmai[j];
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
	public void maximize(SuffStats suffStats) {
		double[] ss0 = suffStats.getSs0();
		double[][] ss1 = suffStats.getSs1();
		double[][] ss2 = suffStats.getSs2();
		double numSmps = 0;
		for (int i=0; i<nMix; i++) {
			numSmps += ss0[i];
		}
		for (int i=0; i<nMix; i++) {
			pi[i] = ss0[i]/numSmps;
			for (int j=0; j<dim; j++) {
				mu[i][j] = ss1[i][j]/ss0[i];
				sigma[i][j] = ss2[i][j]/ss0[i] - mu[i][j]*mu[i][j];
			}
		}
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
	
	public double[][] getMeans() {
		return mu;
	}
	
	public double[][] getCovs() {
		return sigma;
	}

	public double[] getPi() {
		return pi;
	}

	public void setPi(double[] pi) {
		this.pi = pi;
	}

	public double[][] getMu() {
		return mu;
	}

	public void setMu(double[][] mu) {
		this.mu = mu;
	}

	public double[][] getSigma() {
		return sigma;
	}

	public void setSigma(double[][] sigma) {
		this.sigma = sigma;
	}
	
	

	
}
