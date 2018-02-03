package edu.sdsc.mmtf.spark.webfilters.demos;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.io.MmtfReader;
import edu.sdsc.mmtf.spark.mappers.StructureToPolymerChains;
import edu.sdsc.mmtf.spark.webfilters.PdbjMineSearch;

/**
 * PDBj Mine 2 RDB complex query of PDB and SIFTS data, followed by MMTF filtering using pdbid & chain name
 *   Additional filtering can be done on the client side, in addition the obtained data can also be reused (so not only filtering, but also data gathering)
 *   
 * @author Gert-Jan Bekker
 */
public class SiftsQuery 
{
    public static void main( String[] args ) throws IOException
    {
        // goal: use an sql query to get a list of pdbids, then filter the MMTF-DB to only include those entries
    	
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(SiftsQuery.class.getSimpleName());
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		String path = System.getProperty("MMTF_FULL");
	    if (path == null) {
	    	System.err.println("Environment variable for Hadoop sequence file has not been set");
	    	System.exit(-1);
	    }
	    
	    // read PDB in MMTF format
	    JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader.readSequenceFile(path, sc);

		// Retrieve PDB chain sequences matching to the Pfam accession "PF00046" (Homeobox) 
		// and having a resolution better than 2.0 Angstrom and a sequence length greater than or equal to 58 (residues)
        // https://pdbj.org/mine/sql?query=SELECT+concat(s.pdbid%2C+%27.%27%2C+s.chain)+as+structureChainId%2C+s.*%2C+r.ls_d_res_high+as+reso%2C%0A+++++LENGTH(p.pdbx_seq_one_letter_code_can)+as+len%2C%0A+++++(%27%3E%27+%7C%7C+s.pdbid+%7C%7C+s.chain)+as+header%2C%0A+++++replace(p.pdbx_seq_one_letter_code_can%2CE%27%5Cn%27%2C%27%27)+as+aaseq%0AFROM+sifts.pdb_chain_pfam+s%0AJOIN+refine+r+on+r.pdbid+%3D+s.pdbid%0AJOIN+entity_poly+p+on+p.pdbid+%3D+s.pdbid%0A+++++AND+s.chain+%3D+ANY(regexp_split_to_array(p.pdbx_strand_id%2C%27%2C%27))%0AWHERE+pfam_id+%3D+%27PF00046%27%0AAND+r.ls_d_res_high+%3C+2.0%0AAND+LENGTH(p.pdbx_seq_one_letter_code_can)+%3E%3D+58%0AORDER+BY+reso%2C+len%2Cs.chain
		String sql = 	"SELECT concat(s.pdbid, '.', s.chain) as \"structureChainId\", s.*, r.ls_d_res_high as reso,"+ // manually build a pdbid.chainid column and name it "structureChainId" (quotes are required to make it case sensitive), so that this column can be used for filtering the MMTF data on chain-level  
							"LENGTH(p.pdbx_seq_one_letter_code_can) as len, " +
							"('>' || s.pdbid || s.chain) as header, " +
							"replace(p.pdbx_seq_one_letter_code_can,E'\n','') as aaseq " + // the replace here is required because spark's csv parser cannot handle newlines properly (should be fixed in 2.2)
						"FROM sifts.pdb_chain_pfam s " +
						"JOIN refine r on r.pdbid = s.pdbid " +
						"JOIN entity_poly p on p.pdbid = s.pdbid " +
						"AND s.chain = ANY(regexp_split_to_array(p.pdbx_strand_id,',')) " +
						"WHERE pfam_id = 'PF00046' " +
							"AND r.ls_d_res_high < 2.0 " +
						"AND LENGTH(p.pdbx_seq_one_letter_code_can) >= 58 " +
						"ORDER BY reso, len,s.chain ";
        PdbjMineSearch search = new PdbjMineSearch(sql, "structureChainId", true); // structureChainId is used for filtering the MMTF data and chain-level is set to true
//        search.dataset.show(10); // output the returned data (10 items) => this can be further filtered or used directly in your service
        
        
        System.out.println("Number of entries in MMTF library matching query: "+pdb.flatMapToPair(new StructureToPolymerChains()).filter(search).count());
        
        sc.close();
        
    }
}
