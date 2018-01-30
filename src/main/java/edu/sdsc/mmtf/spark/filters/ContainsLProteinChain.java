package edu.sdsc.mmtf.spark.filters;

import org.apache.spark.api.java.function.Function;
import org.rcsb.mmtf.api.StructureDataInterface;

import scala.Tuple2;

/**
 * This filter returns entries that contain polymer chain(s) made of L-amino acids. 
 * The default constructor returns entries that contain at least one 
 * polymer chain that is an L-protein. If the "exclusive" flag is set to true 
 * in the constructor, all polymer chains must be L-proteins. For a multi-model structure,
 * this filter only checks the first model.
 * 
 * @author Peter Rose
 * @since 0.1.0
 *
 */
public class ContainsLProteinChain implements Function<Tuple2<String, StructureDataInterface>, Boolean> {
	private static final long serialVersionUID = -2323293283758321260L;
	private ContainsPolymerChainType filter;

	/**
	 * Default constructor matches any entry that contains at least one L-protein chain.
	 * As an example, an L-protein/DNA complex passes this filter.
	 */
	public ContainsLProteinChain() {
		this(false);
	}
	
	/**
	 * Optional constructor that can be used to filter entries that exclusively contain L-protein chains.
	 * For example, with "exclusive" set to true, an L-protein/DNA complex does not pass this filter.
	 * @param exclusive if true, only return entries that exclusively contain L-protein chains
	 */
	public ContainsLProteinChain(boolean exclusive) {
		this.filter = new ContainsPolymerChainType(exclusive, 
				ContainsPolymerChainType.L_PEPTIDE_LINKING, 
				ContainsPolymerChainType.PEPTIDE_LINKING);
	}
	
	@Override
	public Boolean call(Tuple2<String, StructureDataInterface> t) throws Exception {
		return filter.call(t);
	}
}
