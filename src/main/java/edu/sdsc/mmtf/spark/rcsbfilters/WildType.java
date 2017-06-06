package edu.sdsc.mmtf.spark.rcsbfilters;

import java.io.IOException;

import org.apache.spark.api.java.function.Function;
import org.rcsb.mmtf.api.StructureDataInterface;

import scala.Tuple2;

/**
 * This filter returns entries that contain wild type protein chains.
 * polymer chain(s) made of L-amino acids.  If the "exclusive" flag is set to true 
 * in the constructor, all polymer chains must be L-proteins. For a multi-model structure,
 * this filter only checks the first model.
 * 
 * @author Peter Rose
 *
 */
public class WildType implements Function<Tuple2<String, StructureDataInterface>, Boolean> {
	private static final long serialVersionUID = -2323293283758321260L;
	
	public static int SEQUENCE_COVERAGE_100 = 100;
	public static int SEQUENCE_COVERAGE_95 = 95;
	public static int SEQUENCE_COVERAGE_90 = 90;
	public static int SEQUENCE_COVERAGE_85 = 85;
	public static int SEQUENCE_COVERAGE_80 = 80;
	public static int SEQUENCE_COVERAGE_75 = 75;
	public static int SEQUENCE_COVERAGE_70 = 70;
	public static int SEQUENCE_COVERAGE_65 = 65;
	public static int SEQUENCE_COVERAGE_60 = 60;
	
	private AdvancedQuery filter;

	/**
	 * Default constructor matches any entry that contains at least one L-protein chain.
	 * As an example, an L-protein/DNA complex passes this filter.
	 * @throws IOException 
	 */
	public WildType(boolean includeExpressionTags) throws IOException {
		String query = "<orgPdbQuery><queryType>org.pdb.query.simple.WildTypeProteinQuery</queryType>";
		if (includeExpressionTags) {
			query = query +   "<includeExprTag>Y</includeExprTag>";
		} else {
			query = query +   "<includeExprTag>N</includeExprTag>";
		}
		query = query + "</orgPdbQuery>";
		filter = new AdvancedQuery(query);
	}
	
	/**
	 * Default constructor matches any entry that contains at least one L-protein chain.
	 * As an example, an L-protein/DNA complex passes this filter.
	 * @throws IOException 
	 */
	public WildType(boolean includeExpressionTags, int percentSequenceCoverage) throws IOException {
		String query = "<orgPdbQuery><queryType>org.pdb.query.simple.WildTypeProteinQuery</queryType>";
		if (includeExpressionTags) {
			query = query + "<includeExprTag>Y</includeExprTag>";
		} else {
			query = query + "<includeExprTag>N</includeExprTag>";
		}
	    query = query + "<percentSeqAlignment>";
	    query = query +	percentSequenceCoverage;
	    query = query + "</percentSeqAlignment>";
		query = query + "</orgPdbQuery>";
		filter = new AdvancedQuery(query);
	}
	
	@Override
	public Boolean call(Tuple2<String, StructureDataInterface> t) throws Exception {
		return filter.call(t);
	}
}
