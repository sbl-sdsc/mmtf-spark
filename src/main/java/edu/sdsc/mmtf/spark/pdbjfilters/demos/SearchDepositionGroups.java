package edu.sdsc.mmtf.spark.pdbjfilters.demos;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.io.MmtfReader;
import edu.sdsc.mmtf.spark.pdbjfilters.MineSearch;

/**
 * PDBj Mine 2 RDB group deposition query and MMTF filtering using pdbid
 *  This query also does a simple join with the brief_summary table to filter by resolution
 *  
 *  @author Gert-Jan Bekker
 */
public class SearchDepositionGroups 
{
    public static void main( String[] args ) throws IOException
    {
        // goal: use an sql query to get a list of pdbids, then filter the MMTF-DB to only include those entries
    	
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(SearchDepositionGroups.class.getSimpleName());
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		String path = System.getProperty("MMTF_FULL");
	    if (path == null) {
	    	System.err.println("Environment variable for Hadoop sequence file has not been set");
	    	System.exit(-1);
	    }
	    
	    // read PDB in MMTF format
	    JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader.readSequenceFile(path, sc);

	    // medium complexity query: gets all the pdb entries belonging to the deposition group G_1002001 with a resolution better than 1.5 A
	    // https://pdbj.org/mine/sql?query=select+pdbx_deposit_group.pdbid+from+pdbx_deposit_group+join+brief_summary+on+pdbx_deposit_group.pdbid%3Dbrief_summary.pdbid+where+group_id%3D%27G_1002001%27+and+resolution+%3C+1.5
        String sql = "select pdbx_deposit_group.pdbid from pdbx_deposit_group join brief_summary on pdbx_deposit_group.pdbid=brief_summary.pdbid where group_id='G_1002001' and resolution < 1.5";
		
        // simply run the query
        MineSearch search = new MineSearch(sql); // if no further parameters are given, `pdbid` column (pdbx_deposit_group.pdbid was selected, but is returned as pdbid) will be used for filtering without chain-level
        
        System.out.println("Number of entries in MMTF library matching query: "+pdb.filter(search).keys().count()+"/"+search.dataset.count());
		
        sc.close();
        
    }
}
