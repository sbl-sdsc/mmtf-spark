package edu.sdsc.mmtf.spark.webfilters.demos;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.io.MmtfReader;
import edu.sdsc.mmtf.spark.webfilters.MineSearch;

/**
 * PDBj Mine 2 RDB simple query and MMTF filtering using pdbid
 *
 * @author Gert-Jan Bekker
 */
public class SimpleQuery 
{
    public static void main( String[] args ) throws IOException
    {
        // goal: use an sql query to get a list of pdbids, then filter the MMTF-DB to only include those entries
    	
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(SimpleQuery.class.getSimpleName());
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		String path = System.getProperty("MMTF_FULL");
	    if (path == null) {
	    	System.err.println("Environment variable for Hadoop sequence file has not been set");
	    	System.exit(-1);
	    }
	    
	    // read PDB in MMTF format
	    JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader.readSequenceFile(path, sc);

	    // very simple query; this gets the pdbids for all entries modified since 2016-06-28 with a resulution better than 1.5 A
        String sql = "select pdbid from brief_summary where modification_date >= '2016-06-28' and resolution < 1.5";
		
        // simply run the query
        MineSearch search = new MineSearch(sql); // if no further parameters are given, `pdbid` column will be used for filtering without chain-level
        
        System.out.println("Number of entries in MMTF library matching query: "+pdb.filter(search).keys().count()+"/"+search.dataset.count());
		
        sc.close();
        
    }
}
