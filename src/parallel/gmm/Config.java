/*
 * Author: Man-Wai MAK, Dept. of EIE, The Hong Kong Polytechnic University
 * Version: 1.0
 * Date: March 2015
 * 
 * This file is subject to the terms and conditions defined in
 * file 'license.txt', which is part of this source code package.
*/

package parallel.gmm;

public class Config {
	public static final int NUM_MIX = 256;
	public static final int DIM = 60;
	public static final String nameNode = "hdfs://101.1.1.107:8020";
	public static final String GMM_FILE = nameNode + "/user/mwmak/stats/gmm.txt";
}
