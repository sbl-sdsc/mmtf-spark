package edu.sdsc.mmtf.spark.filters;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.spark.api.java.function.Function;
import org.rcsb.mmtf.api.StructureDataInterface;
import org.rcsb.mmtf.encoder.EncoderUtils;

import scala.Tuple2;

/**
 * This filter returns entries that contain chains made of the specified 
 * monomer types. The default constructor returns entries that contain at least
 * one chain that matches the conditions. If the "exclusive" flag is set to true 
 * in the constructor, all chains must match the conditions. For a multi-model 
 * structure, this filter only checks the first model.
 * 
 * 
 * http://mmcif.wwpdb.org/dictionaries/mmcif_mdb.dic/Items/_chem_comp.type.html
 * 
 * @author Peter Rose
 *
 */
public class ContainsPolymerType implements Function<Tuple2<String, StructureDataInterface>, Boolean> {
	private static final long serialVersionUID = -2323293283758321260L;
	private boolean exclusive = false;
	private Set<String> entityTypes;

	/**
	 * Default constructor matches any entry that contains a chain with only
	 * the specified monomer types:
	 * 
	 * TODO these should be in upper case
	 * D-peptide COOH carboxy terminus	
	 * D-peptide NH3 amino terminus	
	 * D-peptide linking	
	 * D-saccharide	
	 * D-saccharide 1,4 and 1,4 linking	
	 * D-saccharide 1,4 and 1,6 linking	
	 * DNA OH 3 prime terminus	
	 * DNA OH 5 prime terminus	
	 * DNA linking	
	 * L-peptide COOH carboxy terminus	
	 * L-peptide NH3 amino terminus	
	 * L-peptide linking	
	 * L-saccharide	
	 * L-saccharide 1,4 and 1,4 linking	
	 * L-saccharide 1,4 and 1,6 linking	
	 * RNA OH 3 prime terminus	
	 * RNA OH 5 prime terminus	
	 * RNA linking	
	 * non-polymer	
	 * other	
	 * saccharide
	 */
	public ContainsPolymerType(String...monomerTypes) {
        this(false, monomerTypes);
	}
	
	/**
	 * Optional constructor that can be used to filter entries that exclusively match all chains.
	 * @param exclusive if true, all chains must match the specified monomer types.
	 */
	public ContainsPolymerType(boolean exclusive, String...entityTypes) {
		this.exclusive = exclusive;
		this.entityTypes = new HashSet<>(Arrays.asList(entityTypes));
	}
	
	@Override
	public Boolean call(Tuple2<String, StructureDataInterface> t) throws Exception {
		StructureDataInterface structure = t._2;

		boolean containsPolymer = false;
		boolean globalMatch = false;
		int numChains = structure.getChainsPerModel()[0]; // only check first model

		for (int i = 0, groupCounter = 0; i < numChains; i++){
			boolean match = true;	
			String chainType = EncoderUtils.getTypeFromChainId(structure, i);
			boolean polymer = chainType.equals("polymer");

			if (polymer) {
				containsPolymer = true;
			} else {
				match = false;
			}

			for (int j = 0; j < structure.getGroupsPerChain()[i]; j++, groupCounter++) {			
				if (match && polymer) {
					int groupIndex = structure.getGroupTypeIndices()[groupCounter];
					String type = structure.getGroupChemCompType(groupIndex);
					match = entityTypes.contains(type);
				}
			}

			if (polymer && match && ! exclusive) {
				return true;
			}
			if (polymer && ! match && exclusive) {
				return false;
			}
			
			if (match) {
				globalMatch = true;
			}
		}
			
		return globalMatch && containsPolymer;
	}
}
