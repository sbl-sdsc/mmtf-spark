package edu.sdsc.mmtf.spark.filters;

import org.apache.spark.api.java.function.Function;
import org.rcsb.mmtf.api.StructureDataInterface;

import scala.Tuple2;

/**
 * This filter returns entries that contain protein chain(s) made of D-amino acids. 
 * The default constructor returns entries that contain at least one 
 * polymer chain that is an D-protein. If the "exclusive" flag is set to true 
 * in the constructor, all polymer chains must be D-proteins. For a multi-model structure,
 * this filter only checks the first model.
 * 
 * @author Peter Rose
 * @since 0.1.0
 *
 */
public class ContainsDProteinChain implements Function<Tuple2<String, StructureDataInterface>, Boolean> {
	private static final long serialVersionUID = -2323293283758321260L;
	private ContainsPolymerChainType filter;

	/**
	 * Default constructor matches any entry that contains at least one D-protein chain.
	 * As an example, a D-protein/DNA complex passes this filter.
	 */
	public ContainsDProteinChain() {
		this(false);
	}
	
	/**
	 * Optional constructor that can be used to filter entries that exclusively contain D-protein chains.
	 * For example, with "exclusive" set to true, a D-protein/DNA complex does not pass this filter.
	 * @param exclusive if true, only return entries that are exclusively contain D-protein chains
	 */
	public ContainsDProteinChain(boolean exclusive) {
		this.filter = new ContainsPolymerChainType(exclusive, 
				ContainsPolymerChainType.D_PEPTIDE_LINKING, ContainsPolymerChainType.PEPTIDE_LINKING);
	}
	
	@Override
	public Boolean call(Tuple2<String, StructureDataInterface> t) throws Exception {
		return filter.call(t);
	}
}
