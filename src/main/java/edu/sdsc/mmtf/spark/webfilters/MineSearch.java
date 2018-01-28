package edu.sdsc.mmtf.spark.webfilters;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.rcsb.mmtf.api.StructureDataInterface;

import scala.Tuple2;


/**
 * This filter runs an PDBj Mine 2 Search web service using an SQL query.
 * 
 * <p>See <a href="https://pdbj.org/help/mine2-sql"> Mine 2 SQL</a>
 * <p>Design queries using the <a href="https://pdbj.org/mine/sql">PDBj Mine 2 query service</a>.
 * 
 * @author Gert-Jan Bekker
 *
 */
public class MineSearch implements Function<Tuple2<String, StructureDataInterface>, Boolean> {
	private static final long serialVersionUID = -4794067375376198086L;
	public static final String SERVICELOCATION="https://pdbj.org/rest/mine2_sql";
	public Set<String> pdbIds;
	public Dataset<Row> dataset;
	public boolean chainLevel;

	/**
	 * Fetches data using the PDBj Mine 2 SQL service
	 * @param sqlQuery query in SQL format
	 * @throws IOException
	 */
	
	public MineSearch(String sqlQuery) throws IOException {
		this(sqlQuery, "pdbid", false);
	}
	
	public MineSearch(String sqlQuery, String pdbidField) throws IOException {
		this(sqlQuery, pdbidField, false);
    }
	
	
	public MineSearch(String sqlQuery, String pdbidField, Boolean chainLevel) throws IOException {
		this.chainLevel = chainLevel;
		String encodedSQL = URLEncoder.encode(sqlQuery,"UTF-8");
		
		URL u = new URL(SERVICELOCATION+"?format=csv&q="+encodedSQL);
		InputStream in = u.openStream();
		
		// save as a temporary CSV file
		Path tempFile = Files.createTempFile(null, ".csv");
		Files.copy(in, tempFile, StandardCopyOption.REPLACE_EXISTING);
		in.close();
		
		SparkSession spark = SparkSession.builder().getOrCreate();
		
		// load temporary CSV file into Spark dataset
	   dataset = spark.read()
			          .format("csv")
			          .option("header", "true")
			          .option("inferSchema", "true")
			          .option("parserLib", "UNIVOCITY") // <-- This is the configuration that solved the issue.
			          .load(tempFile.toString());
	   
		pdbIds = new HashSet<>();
		// check whether there even is a pdbid field...
		if (Arrays.asList(dataset.columns()).contains(pdbidField)) {
		  List<Row> rows = dataset.select(pdbidField).collectAsList();	
          for (Row r: rows) pdbIds.add(r.getString(0).toUpperCase());
		}
	}
	
	@Override
	public Boolean call(Tuple2<String, StructureDataInterface> t) throws Exception {
		boolean match = pdbIds.contains(t._1);
		
		// if results are PDB IDs, but the keys contains chain names,
		// then truncate the chain name before matching (e.g., 4HHB.A -> 4HHB)
		if (!chainLevel && !match && t._1.length() > 4) match = pdbIds.contains(t._1.substring(0,4));

		return match;
	}

}
