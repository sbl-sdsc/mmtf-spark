package edu.sdsc.mmtf.spark.filters;

import org.apache.spark.api.java.function.Function;
import org.rcsb.mmtf.api.StructureDataInterface;

import scala.Tuple2;

/**
 * This filter returns entries that contain chain(s) made of linear and branched D-saccharides. 
 * The default constructor returns entries that contain at least one 
 * polymer chain that is a D-saccharides. If the "exclusive" flag is set to true 
 * in the constructor, all polymer chains must be D-saccharides. For a multi-model structure,
 * this filter only checks the first model.
 * 
 * Note: Since the PDB released PDBx/mmCIF version 5.0 in July 2017, it appears that
 * all polysaccharides have been converted to monomers. Therefore, this filter
 * does not return any results.
 * 
 * @author Peter Rose
 * @since 0.1.0
 *
 */
public class ContainsDSaccharideChain implements Function<Tuple2<String, StructureDataInterface>, Boolean> {
	private static final long serialVersionUID = -2323293283758321260L;
	private ContainsPolymerChainType filter;

	/**
	 * Default constructor matches any entry that contains at least one chain made of D-saccharides.
	 * As an example, a glycosylated protein complex passes this filter.
	 */
	public ContainsDSaccharideChain() {
		this(false);
	}
	
	/**
	 * Optional constructor that can be used to filter entries that exclusively contain D-saccharide chains.
	 * For example, with "exclusive" set to true, an D-saccharide/protein does not pass this filter.
	 * @param exclusive if true, only return entries that are exclusively contain D-saccharide chains
	 */
	public ContainsDSaccharideChain(boolean exclusive) {
		this.filter = new ContainsPolymerChainType(exclusive, 
				ContainsPolymerChainType.D_SACCHARIDE, 
				ContainsPolymerChainType.SACCHARIDE, // should this be excluded?
				ContainsPolymerChainType.D_SACCHARIDE_14_and_14_LINKING,
				ContainsPolymerChainType.D_SACCHARIDE_14_and_16_LINKING);
	}
	
	@Override
	public Boolean call(Tuple2<String, StructureDataInterface> t) throws Exception {
		return filter.call(t);
	}
}
