package largespace.business;
import largespace.clustering.Column;
import largespace.clustering.FromCluster;

import com.beust.jcommander.Parameter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public final class Options {
	@Parameter(names = "-FOLDER_C_OUTPUT", required = false)
    public String FOLDER_C_OUTPUT;
	
    @Parameter(names = "-FILE_C_OUTPUT", required = true)
    public String FILE_C_OUTPUT;
    
    @Parameter(names = "-FILE_CLMN_OUTPUT", required = false)
    public String FILE_CLMN_OUTPUT;
    
    @Parameter(names = "-FILE_TBL_OUTPUT", required = false)
    public String FILE_TBL_OUTPUT;

    @Parameter(names = "-FILE_INPUT", required = true)
    public String FILE_INPUT;
   

    @Parameter(names = "-FILE_PRE_OUTPUT", required = false)
    public String FILE_PRE_OUTPUT;

    @Parameter(names = "-FILE_TABLES", required = false)
    public String FILE_TABLES;

    @Parameter(names = "-FILE_MIN_MAX", required = false)
    public String FILE_MIN_MAX;

    @Parameter(names = "-PREPROCESS")
    public Boolean PREPROCESS = false;
    
    @Parameter(names = "-EPSILON")
    public double EPSILON = 0.8;

    @Parameter(names = "-MIN_PTS")
    public int MIN_PTS = 20;


    @Parameter(names = "-MAX_POINTS")
    public int MAX_POINTS = 12400000;


    @Parameter(names = "-D_DIFFERENT_FIELDS")
    public double D_DIFFERENT_FIELDS = 1;

    @Parameter(names = "-D_SAME_FIELDS")
    public double D_SAME_FIELDS = 0.3;


    @Parameter(names = "-FIELD_DELIMITER")
    public String FIELD_DELIMITER = ";";


    // contains min-max of numerical fields
    public Map<String, double[]> COLS_MIN_MAX;
    public ArrayList<String> TABLES;
    public Map<String, Table> TABLESWITHCOUNT = new TreeMap<String, Table>();
    public Map<String, Column> COLUMNS_DISTRIBUTION = new TreeMap<String, Column>();
    
    public Map<String, Column> PENALTY_COLUMNS_DISTRIBUTION = new TreeMap<String, Column>();
    
    public long Trash = 0;
    
    public long MAX_DISTINCT_VALUES_TO_BE_DICTIONARY_FIELD = 2000;
    public double MIN_PART_TO_BE_EMISSION = 0.1;
    
    public ArrayList<FromCluster> FromClusters = new ArrayList<FromCluster>();

    public String FILE_START_POSITION = "_position";
    
}