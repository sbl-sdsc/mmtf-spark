package edu.sdsc.mmtf.spark.webfilters;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.datasets.CustomReportService;
import scala.Tuple2;

/**
 * This filter runs an SQL query on specified PDB metadata and annotation fields retrieved using
 * RCSB PDB RESTful web services. The fields are then queried and the resulting PDB IDs are 
 * used to filter the data. The input to the filter consists of an SQL WHERE clause, and list 
 * of data columns available from RCSB PDB web services.
 * 
 * <p>See <a href="https://www.rcsb.org/pdb/results/reportField.do"> list of supported
 * field names.</a>
 * 
 * <p>See <a href="https://www.w3schools.com/sql/sql_where.asp"> for examples of
 * SQL WHERE clauses.</a>
 * 
 * <p>Example: find PDB entries with Enzyme classification number 2.7.11.1
 * and source organism Homo sapiens:
 * 
 * <pre><code>
 *      JavaPairRDD<String, StructureDataInterface> pdb = ...
 *      String whereClause = "WHERE ecNo='2.7.11.1' AND source='Homo sapiens'";
 *      pdb = pdb.filter(new CustomReportQuery(whereClause, "ecNo","source"));
 * </code></pre>
 * 
 * @author Peter Rose
 * @since 0.1.0
 *
 */
public class CustomReportQuery implements Function<Tuple2<String, StructureDataInterface>, Boolean> {
	private static final long serialVersionUID = -4794067375376198086L;
	private Set<String> pdbIds;
	private boolean chainLevel;

	/**
	 * Filters using an SQL query on the specified fields
	 * @param whereClause WHERE Clause of SQL statement
	 * @param fields one or more field names to be used in query
	 * @throws IOException
	 */
	public CustomReportQuery(String whereClause, String... fields) throws IOException {
		
		// get requested data columns
		Dataset<Row> dataset = CustomReportService.getDataset(fields);
		
		// check if the results contain chain level data
		chainLevel = Arrays.asList(dataset.columns()).contains("structureChainId");
	
		// create a temporary view of the dataset
		dataset.createOrReplaceTempView("table");
		
		// run SQL query
		if (chainLevel) {
			// for chain level data
			String sql = "SELECT structureChainId, structureId, chainId FROM table " + whereClause;
			Dataset<Row> results = dataset.sparkSession().sql(sql);
			// add both PDB entry and chain level data, so chain-based data can be filtered
			pdbIds = new HashSet<String>(results.distinct().toJavaRDD().map(r -> r.getString(0)).collect());
			pdbIds.addAll(results.distinct().toJavaRDD().map(r -> r.getString(1)).collect());
		} else {
			// for PDB entry level data
			String sql = "SELECT structureId FROM table " + whereClause;
			Dataset<Row> results = dataset.sparkSession().sql(sql);
			pdbIds = new HashSet<String>(results.distinct().toJavaRDD().map(r -> r.getString(0)).collect());
		}
	}
	
	@Override
	public Boolean call(Tuple2<String, StructureDataInterface> t) throws Exception {
		boolean match = pdbIds.contains(t._1);
		
		// if results are PDB IDs, but the keys contains chain names,
		// then truncate the chain name before matching (e.g., 4HHB.A -> 4HHB)
		if (!chainLevel && !match && t._1.length() > 4) {
			return pdbIds.contains(t._1.substring(0,4));
		}
		return match;
	}
}
