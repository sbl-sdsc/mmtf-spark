package edu.sdsc.mmtf.spark.webfilters;

import java.io.IOException;

import org.apache.spark.api.java.function.Function;
import org.rcsb.mmtf.api.StructureDataInterface;

import scala.Tuple2;

/**
 * This filter returns entries that contain groups with specified chemical structure (SMILES string).
 * This chemical structure query supports for query: exact, similar, substructure, and superstructure. 
 * For details see 
 * <a href="http://www.rcsb.org/pdb/staticHelp.do?p=help/advancedsearch/chemSmiles.html">Chemical Structure Search</a>.
 * 
 * @author Peter Rose
 *
 */
public class SequenceSimilarity implements Function<Tuple2<String, StructureDataInterface>, Boolean> {
	private static final long serialVersionUID = -8647960961587233458L;
	private AdvancedQuery filter;
	
	public static final String EXACT = "Exact";
	public static final String SIMILAR = "Similar";
	public static final String SUBSTRUCTURE = "Substructure";
	public static final String SUPERSTRUCTURE = "Superstructure";

	/**
	 * Default constructor to setup filter that matches any entry with at least one chemical component
	 * that is a substructure of the specified SMILES string. For details see 
     * <a href="http://www.rcsb.org/pdb/staticHelp.do?p=help/advancedsearch/chemSmiles.html">Chemical Structure Search</a>.
	 * 
	 * @throws IOException 
	 */
	public SequenceSimilarity(String smiles) throws IOException {
		this(smiles, SequenceSimilarity.SUBSTRUCTURE, 0);
	}
	
	/**
	 *  Constructor to setup filter that matches any entry with at least one chemical component
	 *  that matches the specified SMILES string using the specified query type. For details see 
     *  <a href="http://www.rcsb.org/pdb/staticHelp.do?p=help/advancedsearch/chemSmiles.html">Chemical Structure Search</a>.
	 *  
	 * @param smiles SMILES string representing chemical structure
	 * @param queryType one of the four supported query types:
	 * {@link SequenceSimilarity#EXACT RcsbChemicalStructure.EXACT},
	 * {@link SequenceSimilarity#SIMILAR RcsbChemicalStructure.SIMILAR},
	 * {@link SequenceSimilarity#SUBSTRUCTURE RcsbChemicalStructure.SUBSTRUCTURE},
	 * {@link SequenceSimilarity#SUPERTRUCTURE RcsbChemicalStructure.SUPERSTRUCTURE},
	 * @param percentSimilarity percent similarity for similarity search. This parameter is
	 * ignored for all other query types.
     *
	 * @throws IOException
	 */
	public SequenceSimilarity(String smiles, String queryType, int percentSimilarity) throws IOException {
		if (! (queryType.equals(SUBSTRUCTURE) || queryType.equals(SUPERSTRUCTURE) ||
			queryType.equals(SIMILAR) || queryType.equals(EXACT)) ) {
				throw new IllegalArgumentException("Invalid search type: " + queryType);
		}
			
		String query = "<orgPdbQuery>" +
                           "<queryType>org.pdb.query.simple.ChemSmilesQuery</queryType>" +
                	       "<smiles>" + smiles + "</smiles>" +
                	       "<searchType>" + queryType + "</searchType>" +
                	       "<similarity>" + percentSimilarity + "</similarity>" +
                	       "<polymericType>Any</polymericType>" +
	                   "</orgPdbQuery>";
		filter = new AdvancedQuery(query);
	}
	
	@Override
	public Boolean call(Tuple2<String, StructureDataInterface> t) throws Exception {
		return filter.call(t);
	}
}
