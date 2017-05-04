package edu.sdsc.mmtf.spark.filters;

import org.apache.spark.api.java.function.Function;
import org.rcsb.mmtf.api.StructureDataInterface;
import org.rcsb.mmtf.encoder.EncoderUtils;

import scala.Tuple2;

/**
 * This filter returns entries that contains protein chain(s) made of L-amino acids 
 * (L-proteins). The default constructor returns entries that contain at least one 
 * polymer chain that is an L-protein. If the "exclusive" flag is set to true 
 * in the constructor, all polymer chains must be L-proteins. For a multi-model structure,
 * this filter only checks the first model.
 * 
 * @author Peter Rose
 *
 */
public class ContainsLProteinChain implements Function<Tuple2<String, StructureDataInterface>, Boolean> {
	private static final long serialVersionUID = -2323293283758321260L;
	private boolean exclusive = false;

	/**
	 * Default constructor matches any entry that contains at least one L-protein chain.
	 * As an example, an L-protein/L-DNA complex passes this filter.
	 */
	public ContainsLProteinChain() {
		this.exclusive = false;
	}
	
	/**
	 * Optional constructor that can be used to filter entries that exclusively contain L-protein chains.
	 * For example, with "exclusive" set to true, an L-protein/L-DNA complex does not pass this filter.
	 * @param exclusive if true, only return entries that are exclusively contain L-protein chains
	 */
	public ContainsLProteinChain(boolean exclusive) {
		this.exclusive = exclusive;
	}
	
	@Override
	public Boolean call(Tuple2<String, StructureDataInterface> t) throws Exception {
		StructureDataInterface structure = t._2;

		// if there is only one entity, we can just check the list of chemical components
		if (structure.getNumEntities() == 1) {
			for (int index: structure.getGroupTypeIndices()) {
				String type = structure.getGroupChemCompType(index);
				if ( !(type.equals("L-PEPTIDE LINKING") || type.equals("PEPTIDE LINKING")) ) {
					return false;
				}
			}
		}
		
		// for multiple entities, we need to loop through the 
		// data structure to check each polymer chain
		boolean match = true;
		
		int chainCounter = -1;
		int groupCounter = -1;
		
		int numChains = structure.getChainsPerModel()[0]; // only check first model
		for (int i = 0; i < numChains; i++){
			chainCounter++;
			
			String chainType = EncoderUtils.getTypeFromChainId(structure, chainCounter);
			boolean polymer = chainType.equals("polymer");

			for(int j = 0; j <structure.getGroupsPerChain()[chainCounter]; j++){
				groupCounter++;
				int groupIndex = structure.getGroupTypeIndices()[groupCounter];
				String type = structure.getGroupChemCompType(groupIndex);
				
				if (polymer && !(type.equals("L-PEPTIDE LINKING") || type.equals("PEPTIDE LINKING")) ) {
					match = false;
				}
			}

			if (polymer && match && !exclusive) {
				return true;
			}
		}
			
		return match;
	}
}
