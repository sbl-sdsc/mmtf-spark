package edu.sdsc.mmtf.spark.pdbjfilters.demos;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.io.MmtfReader;
import edu.sdsc.mmtf.spark.pdbjfilters.MineSearch;

/**
 * PDBj Mine 2 RDB keyword search query and MMTF filtering using pdbid
 *  This filter searches the `keyword` column in the brief_summary table for a keyword and returns a couple of columns for the matching entries
 *  
 *  @author Gert-Jan Bekker
 */
public class KeywordSearch 
{
    public static void main( String[] args ) throws IOException
    {
        // goal: use an sql query to get a list of pdbids, then filter the MMTF-DB to only include those entries
    	
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(KeywordSearch.class.getSimpleName());
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		String path = System.getProperty("MMTF_FULL");
	    if (path == null) {
	    	System.err.println("Environment variable for Hadoop sequence file has not been set");
	    	System.exit(-1);
	    }
	    
	    // read PDB in MMTF format
	    JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader.readSequenceFile(path, sc);

	    // do a keyword search and get the pdbid from the brief_summary table, finally order by the hit_score
        String sql = "select pdbid, resolution, biol_species, db_uniprot, db_pfam, hit_score from keyword_search('porin') order by hit_score desc";
		
        // simply run the query
        MineSearch search = new MineSearch(sql); // if no further parameters are given, `pdbid` column will be used for filtering without chain-level
        search.dataset.show(10); // output the returned data (10 items) => this can be further filtered or used directly in your service
        
        System.out.println("Number of entries in MMTF library matching query: "+pdb.filter(search).keys().count()+"/"+search.dataset.count());
		
        sc.close();
        
    }
}
