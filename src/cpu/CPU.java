package cpu;

public class CPU {
	/*
	 * A method to add extra computation for measuring the performance of the distributed program
	 */
	public static final int NUM_UNIT = 500000;
	public static void wasteCpuTime(int n) {
		for (int i=0; i<n; i++) {
			Math.exp(1.0);
		}
	}
}
