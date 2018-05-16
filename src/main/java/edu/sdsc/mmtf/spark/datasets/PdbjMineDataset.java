package edu.sdsc.mmtf.spark.datasets;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.upper;
import static org.apache.spark.sql.functions.concat;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * PdbjMineDataset runs an SQL query using PDBj Mine 2 Search webservice and
 * returns PDB metadata and SIFTS annotations as a Dataset.
 * 
 * <p>
 * Data are provided through <a href="https://pdbj.org/help/mine2-sql">Mine 2
 * SQL</a>.
 * 
 * <p>
 * Queries can be designed with the interactive
 * <a href="https://pdbj.org/mine/sql">PDBj Mine 2 query service</a>.
 * 
 * <p>
 * PDB metadata are described in the
 * <a href="http://mmcif.wwpdb.org/">PDBx/mmCIF Dictionary</a>. See example
 * {@link edu.sdsc.mmtf.spark.datasets.demos.PdbMetadataDemo}.
 * 
 * <p>
 * SIFTS annotations are available from the "Structure Integration with
 * Function, Taxonomy and Sequence"
 * (<a href="https://www.ebi.ac.uk/pdbe/docs/sifts/overview.html">SIFTS
 * Project</a>). It is the authoritative source of up-to-date residue-level
 * annotation of structures in the PDB with data available in UniProt, IntEnz,
 * CATH, SCOP, GO, InterPro, Pfam and PubMed. See example
 * {@link edu.sdsc.mmtf.spark.datasets.demos.SiftsDataDemo}.
 * 
 * @author Gert-Jan Bekker
 * @since 0.1.0
 *
 */
public class PdbjMineDataset {
	private static final String SERVICELOCATION = "https://pdbj.org/rest/mine2_sql";

	/**
	 * Fetches data using the PDBj Mine 2 SQL service
	 * 
	 * @param sqlQuery
	 *            query in SQL format
	 * @throws IOException
	 */
	public static Dataset<Row> getDataset(String sqlQuery) throws IOException {
		String encodedSQL = URLEncoder.encode(sqlQuery, "UTF-8");

		URL u = new URL(SERVICELOCATION + "?format=csv&q=" + encodedSQL);
		InputStream in = u.openStream();

		// save as a temporary CSV file
		Path tempFile = Files.createTempFile(null, ".csv");
		Files.copy(in, tempFile, StandardCopyOption.REPLACE_EXISTING);
		in.close();

		SparkSession spark = SparkSession.builder().getOrCreate();

		// load temporary CSV file into Spark dataset
		Dataset<Row> ds = spark.read().format("csv").option("header", "true").option("inferSchema", "true")
				// .option("parserLib", "UNIVOCITY")
				.load(tempFile.toString());

		// rename/concatenate columns to assign
		// consistent primary keys to datasets
		List<String> columns = Arrays.asList(ds.columns());

		if (columns.contains("pdbid")) {
			// this project uses upper case pdbids
			ds = ds.withColumn("pdbid", upper(col("pdbid")));

			if (columns.contains("chain")) {
				ds = ds.withColumn("structureChainId", concat(col("pdbid"), lit("."), col("chain")));
				ds = ds.drop("pdbid", "chain");
			} else {
				ds = ds.withColumnRenamed("pdbid", "structureId");
			}
		}

		return ds;
	}
}
