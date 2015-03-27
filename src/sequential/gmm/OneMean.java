package sequential.gmm;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

import cpu.CPU;

/**
 * @author mwmak
 * Find the global mean vectors of a .csv file containing many row vectors.
 * The 1st element (column) is the ID of the vector
 * Example usage:
 *      cd <Workspace>/MapReduce/bin
 * 		java sequential.gmm.OneMean ../matlab/sales_figure.txt
 */

public class OneMean {
	public static void main(String[] args) {
		String datafile = args[0];
		OneMean om = new OneMean();		
		double[] meanVector = om.getMeanVector(datafile);
		for (double e : meanVector) {
			System.out.printf("%.2f ", e);
		}
		System.out.println();
	}
	
	public int getDimension(String datafile) {
		Scanner scanner = null;
		try {
			scanner = new Scanner(new File(datafile));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		String[] token = scanner.nextLine().split("\\s+|,");
		scanner.close();
		return(token.length-1);				// First element is an ID
	}
	
	public double[] getMeanVector(String datafile) {	
		Scanner scanner = null;
		int dim = getDimension(datafile);
		long numVectors = 0;
		double[] meanVector = new double[dim];
		try {
			scanner = new Scanner(new File(datafile));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		while (scanner.hasNextLine()) {
			numVectors++;
			String line = scanner.nextLine();
			String[] token = line.split("\\s+|,");			// Either spacee or ',' as delimiter
			if (token.length > 1) {
				double x[] = new double[token.length - 1]; // 1st element is an ID
				for (int i = 0; i < x.length; i++) {
					x[i] = Double.parseDouble(token[i + 1]);
				}
				accumulate(meanVector, x);
				CPU.wasteCpuTime(CPU.NUM_UNIT);
			}
		}
		for (int i=0; i<dim; i++) {
			meanVector[i] /= numVectors;
		}
		return meanVector;
	}
	
	private void accumulate(double[] m, double[] x) {
		for (int i=0; i<m.length; i++) {
			m[i] += x[i];
		}
	}
}
