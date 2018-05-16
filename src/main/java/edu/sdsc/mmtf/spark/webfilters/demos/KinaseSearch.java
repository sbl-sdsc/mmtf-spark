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
 * Example query for human protein-serine/threonine kinases using
 * SIFTS data retrieved with PDBj Mine 2 webservices.
 * 
 * <p>
 * The "Structure Integration with Function, Taxonomy and Sequence"
 * (<a href="https://www.ebi.ac.uk/pdbe/docs/sifts/overview.html">SIFTS</a>) is
 * the authoritative source of up-to-date residue-level annotation of structures
 * in the PDB with data available in UniProt, IntEnz, CATH, SCOP, GO, InterPro,
 * Pfam and PubMed. See also
 * {@link edu.sdsc.mmtf.spark.datasets.demos.SiftsDataDemo}.
 *
 * <p>
 * Data are provided through <a href="https://pdbj.org/help/mine2-sql">Mine 2
 * SQL</a>
 * 
 * <p>
 * Queries can be designed using the interactive
 * <a href="https://pdbj.org/mine/sql">PDBj Mine 2 query service</a>.
 * 
 * @author Gert-Jan Bekker
 * @author Peter Rose
 * @since 0.2.0
 * 
 */
public class KinaseSearch {
	public static void main(String[] args) throws IOException {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(KinaseSearch.class.getSimpleName());
		JavaSparkContext sc = new JavaSparkContext(conf);

		// query for human protein-serine/threonine kinases using SIFTS data
		String sql = "SELECT t.pdbid, t.chain FROM sifts.pdb_chain_taxonomy AS t  "
				+ "JOIN sifts.pdb_chain_enzyme AS e ON (t.pdbid = e.pdbid AND t.chain = e.chain) "
				+ "WHERE t.scientific_name = 'Homo sapiens' AND e.ec_number = '2.7.11.1'";

		// read PDB in MMTF format, split into polymer chains and search using
		// PdbJMineSearch
		JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader
				.readReducedSequenceFile(sc)
				.flatMapToPair(new StructureToPolymerChains())
				.filter(new PdbjMineSearch(sql));

		System.out.println("Number of entries matching query: " + pdb.count());

		sc.close();
	}
}
