package edu.sdsc.mmtf.spark.webfilters.demos;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.io.MmtfReader;
import edu.sdsc.mmtf.spark.webfilters.PdbjMineSearch;

/**
 * KeywordSearch shows how to select a subset of PDB structures by 
 * a keyword search using the PDBj Mine 2 Search web service.
 * 
 * @author Gert-Jan Bekker
 * @since 0.1.0
 */
public class KeywordSearch {
	public static void main(String[] args) throws IOException {
		
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(KeywordSearch.class.getSimpleName());
		JavaSparkContext sc = new JavaSparkContext(conf);

		String sqlQuery = "SELECT pdbid from keyword_search('porin')";

		// read PDB and filter by keyword search
		JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader
				.readReducedSequenceFile(sc)
				.filter(new PdbjMineSearch(sqlQuery));

		pdb.keys().foreach(k -> System.out.println(k));
		
		System.out.println("Number of entries matching query: " + pdb.count());

		sc.close();
	}
}
