/**
 * 
 */
package edu.sdsc.mmtf.spark.webfilters.demos;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import edu.sdsc.mmtf.spark.io.MmtfReader;
import edu.sdsc.mmtf.spark.webfilters.WildType;

/**
 * This demo selects protein sequences that do not contain mutations in
 * comparison with the reference UniProt sequences.
 * 
 * Expression tags: Some PDB entries include expression tags that were added
 * during the experiment. Select "No" to filter out sequences with expression
 * tags. Percent coverage of UniProt sequence: PDB entries may contain only a
 * portion of the referenced UniProt sequence. The "Percent coverage of UniProt
 * sequence" option defines how much of a UniProt sequence needs to be contained
 * in a PDB entry.
 * 
 * @author Peter Rose
 * @since 0.2.0
 *
 */
public class WildTypeQuery {

	/**
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {

		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(WildTypeQuery.class.getSimpleName());
		JavaSparkContext sc = new JavaSparkContext(conf);

		boolean includeExpressionTags = true;
		int sequenceCoverage = 95;

		long count = MmtfReader.readReducedSequenceFile(sc)
				.filter(new WildType(includeExpressionTags, sequenceCoverage)).count();

		System.out.println(count);

		sc.close();
	}
}
