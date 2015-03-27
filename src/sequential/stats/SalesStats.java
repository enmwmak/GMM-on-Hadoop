package sequential.stats;


import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Scanner;

public class SalesStats {
	private ArrayList<double[]> sales;
	
	public static void main(String[] args) {
		String infile = args[0];
		SalesStats ss = new SalesStats(infile);		
		ss.printStats();
	}
	
	public SalesStats(String infile) {
		Scanner scanner = null;
		try {
			scanner = new Scanner(new File(infile));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		sales = new ArrayList<double[]>();
		while (scanner.hasNextLine()) {
			String line = scanner.nextLine();
			String[] token = line.split(",");
			double custSales[] = new double[token.length-1];
			for (int i=0; i<custSales.length; i++) {
				custSales[i] = Double.parseDouble(token[i+1]);	// token[0] is custID
			}
			sales.add(custSales);
		}
	}
	
	public double getOverallMean() {
		double sum = 0.0;
		long n = 0;
		for (double[] custSales : sales) {
			for (double s : custSales) {
				sum += s;
				n++;
			}
		}
		return sum/n;
	}
	
	public double getCustMean(double[] custSales) {
		double sum = 0.0;
		for (double s : custSales) {
			sum += s;
		}
		return sum / custSales.length;
	}
	
	public double getCustVar(double[] custSales) {
		double mean = getCustMean(custSales);
		double sum = 0;
		for (double s : custSales) {
			double diff = mean - s;
			sum += diff * diff;
		}
		return sum / custSales.length;
	}
	
	public void printStats() {
		int custID = 1;
		for (double[] custSales : sales) {
			System.out.printf("%d: %.1f, %.1f\n", custID++, getCustMean(custSales), getCustVar(custSales));
		}		
	}
	
	public void printAll() {
		for (double[] custSales : sales) {
			for (double s : custSales) {
				System.out.printf("%.0f ", s);
			}
			System.out.println();
		}
	}
	
}
