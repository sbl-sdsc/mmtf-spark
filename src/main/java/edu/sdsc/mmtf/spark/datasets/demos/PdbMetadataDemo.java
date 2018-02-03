package edu.sdsc.mmtf.spark.datasets.demos;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.upper;

import java.io.IOException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import edu.sdsc.mmtf.spark.datasets.PdbjMineDataset;

/**
 * This demo shows how to query metadata from the PDB archive using
 * the PDBj Mine 2 web service. 
 * 
 * <p> This example queries the _citation category in .cif files. Each category
 * represents a table, and the fields represent the database columns to be queried.
 * 
 * <p> Example 100D.cif:
 * # 
 * _citation.id                        primary 
 * _citation.title                     Crystal structure of ...
 * _citation.journal_abbrev            'Nucleic Acids Res.' 
 * _citation.journal_volume            22 
 * _citation.page_first                5466 
 * _citation.page_last                 5476 
 * _citation.year                      1994 
 * _citation.journal_id_ASTM           NARHAD 
 * _citation.country                   UK 
 * _citation.journal_id_ISSN           0305-1048 
 * _citation.journal_id_CSD            0389 
 * _citation.book_publisher            ? 
 * _citation.pdbx_database_id_PubMed   7816639 
 * _citation.pdbx_database_id_DOI      10.1093/nar/22.24.5466 
 *
 * <p>
 * See <a href="https://pdbj.org/help/mine2-sql"> Mine 2 SQL</a>
 * <p>
 * Design queries using the interactive <a href="https://pdbj.org/mine/sql">PDBj Mine 2
 * query service</a>.
 * 
 * @author Peter Rose
 * @author Gert-Jan Bekker
 * @since 0.2.0
 *
 */
public class PdbMetadataDemo {

   public static void main(String[] args) throws IOException {
	   SparkSession spark = SparkSession.builder().master("local[*]").appName(PdbMetadataDemo.class.getSimpleName())
               .getOrCreate();

	   // query the following fields from the _citation category using PDBj's Mine2 web service:
	   // _citation.journal_abbrev, _citation.pdbx_database_id_PubMed, _citation.year 
	   
	   // (note, mixed case column names must be quoted and escaped with \" )
	   String sqlQuery = "SELECT pdbid, journal_abbrev, \"pdbx_database_id_PubMed\", year from citation WHERE id = 'primary'";
	   Dataset<Row>ds = PdbjMineDataset.getDataset(sqlQuery);
	   
	   // rename to standard field name and upper case id for compatibility with this project
	   ds = ds.withColumnRenamed("pdbid", "structureId");
	   ds = ds.withColumn("structureId", upper(col("structureId")));
	   
	   System.out.println("Some example results for query: " + sqlQuery);
	   ds.show(10, false);
	    
	   // filter out unpublished entries (they contain the word "published" in various upper/lower case combinations)
	   ds = ds.filter("UPPER(journal_abbrev) NOT LIKE '%PUBLISHED%'");
	   
	   // print the top 10 journals
	   System.out.println("Top 10 journals publishing PDB structures:");
	   ds.groupBy("journal_abbrev").count().sort(col("count").desc()).show(10, false);
	
	   // filter out entries without a PubMed Id (is -1 if PubMed Id is not available)
	   ds = ds.filter("pdbx_database_id_PubMed > 0");
	   System.out.println("Entries with PubMed Ids: " + ds.count());
	   
	   // show growth of papers in PubMed
	   System.out.println("PubMed Ids per year: ");
	   ds.groupBy("year").count().sort(col("year").desc()).show(10, false);

	   spark.close();
   }
}
