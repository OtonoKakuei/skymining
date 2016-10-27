package largespace.business;

import java.security.SecureRandom;
import java.util.*;

public class Constants 
{
	public static int NUM_POINTS = 12400000;
	public static int MAX_POINTS = 12400000;
	public static int NUM_TRIES = 10000;
	public static double POINTS_THRESHOLD = 0.005;
	public static String FILE_INPUT;
	public static String FILE_PRE_OUTPUT;
	public static String FILE_C_OUTPUT;
	public static String FILE_RUNTIME_OUTPUT;
	public static String FIELD_DELIMITER = ";";
	
	public static long MISC = 0;
	public static double EPSILON = 0.8;
	public static int MIN_PTS = 20;
	public static double OVR = 0.5;
	
	public static Random RAND_GEN = new SecureRandom();
	
	public static double D_SAME_OPERATOR = 0.2;
	public static double D_SAME_FIELDS = 0.3;
	public static double D_DIFFERENT_FIELDS = 1;
	
	// contains min-max of numerical fields
	public static HashMap<String, double[]> map;
	public static ArrayList<String> TABLES = null;
}