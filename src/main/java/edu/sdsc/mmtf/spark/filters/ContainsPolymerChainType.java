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
 * @since 0.1.0
 *
 */
public class ContainsPolymerChainType implements Function<Tuple2<String, StructureDataInterface>, Boolean> {
	private static final long serialVersionUID = -2323293283758321260L;
	private boolean exclusive = false;
	private Set<String> entityTypes;
	
	public static final String D_PEPTIDE_COOH_CARBOXY_TERMINUS = "D-PEPTIDE COOH CARBOXY TERMINUS";
	public static final String D_PEPTIDE_NH3_AMINO_TERMINUS = "D-PEPTIDE NH3 AMINO TERMINUS";	
	public static final String D_PEPTIDE_LINKING = "D-PEPTIDE LINKING";	
	public static final String D_SACCHARIDE = "D-SACCHARIDE";	
	public static final String D_SACCHARIDE_14_and_14_LINKING = "D-SACCHARIDE 1,4 AND 1,4 LINKING";
	public static final String D_SACCHARIDE_14_and_16_LINKING = "D-SACCHARIDE 1,4 AND 1,6 LINKING";	
	public static final String DNA_OH_3_PRIME_TERMINUS = "DNA OH 3 PRIME TERMINUS";
	public static final String DNA_OH_5_PRIME_TERMINUS = "DNA OH 5 PRIME TERMINUS";	
	public static final String DNA_LINKING = "DNA LINKING";	
	public static final String L_PEPTIDE_COOH_CARBOXY_TERMINUS = "L-PEPTIDE COOH CARBOXY TERMINUS";
	public static final String L_PEPTIDE_NH3_AMINO_TERMINUS = "L-PEPTIDE NH3 AMINO TERMINUS";	
	public static final String L_PEPTIDE_LINKING = "L-PEPTIDE LINKING";	
	public static final String L_SACCHARIDE = "L-SACCHARIDE";	
	public static final String L_SACCHARIDE_14_AND_14_LINKING = "L-SACCHARDIE 1,4 AND 1,4 LINKING";	
	public static final String L_SACCHARIDE_14_AND_16_LINKING = "L-SACCHARIDE 1,4 AND 1,6 LINKING";	
	public static final String PEPTIDE_LINKING = "PEPTIDE LINKING";
	public static final String RNA_OH_3_PRIME_TERMINUS = "RNA OH 3 PRIME TERMINUS";
	public static final String RNA_OH_5_PRIME_TERMINUS = "RNA OH 5 PRIME TERMINUS";
	public static final String RNA_LINKING = "RNA LINKING";	
	public static final String NON_POLYMER = "NON-POLYMER";
	public static final String OTHER = "OTHER";	
	public static final String SACCHARIDE = "SACCHARIDE";

	/**
	 * Default constructor matches any entry that contains a chain with only
	 * the specified monomer types:
	 * @param monomerTypes comma separated list of monomer types in a polymer chains
	 * <p>
	 * Frequently occurring monomer types:
	 * {@link ContainsPolymerChainType#L_PEPTIDE_LINKING ContainsPolymerChainType.L_PEPTIDE_LINKING},
	 * {@link ContainsPolymerChainType#D_PEPTIDE_LINKING ContainsPolymerChainType.D_PEPTIDE_LINKING},
	 * {@link ContainsPolymerChainType#DNA_LINKING ContainsPolymerChainType.DNA_LINKING},
	 * {@link ContainsPolymerChainType#RNA_LINKING ContainsPolymerChainType.RNA_LINKING},
	 * {@link ContainsPolymerChainType#NON_POLYMER ContainsPolymerChainType.NON_POLYMER},
	 * <p>
	 * Less frequently occurring monomer types:
	 * {@link ContainsPolymerChainType#D_PEPTIDE_COOH_CARBOXY_TERMINUS ContainsPolymerChainType.D_PEPTIDE_COOH_CARBOXY_TERMINUS},
	 * {@link ContainsPolymerChainType#D_PEPTIDE_NH3_AMINO_TERMINUS ContainsPolymerChainType.D_PEPTIDE_NH3_AMINO_TERMINUS},
	 * {@link ContainsPolymerChainType#D_SACCHARIDE ContainsPolymerChainType.D_SACCHARIDE},
	 * {@link ContainsPolymerChainType#D_SACCHARIDE_14_and_14_LINKING ContainsPolymerChainType.D_SACCHARIDE_14_and_14_LINKING},
	 * {@link ContainsPolymerChainType#D_SACCHARIDE_14_and_16_LINKING ContainsPolymerChainType.D_SACCHARIDE_14_and_16_LINKING},
	 * {@link ContainsPolymerChainType#DNA_OH_3_PRIME_TERMINUS ContainsPolymerChainType.DNA_OH_3_PRIME_TERMINUS},
	 * {@link ContainsPolymerChainType#DNA_OH_5_PRIME_TERMINUS  ContainsPolymerChainType.DNA_OH_5_PRIME_TERMINUS },
	 * {@link ContainsPolymerChainType#L_PEPTIDE_NH3_AMINO_TERMINUS ContainsPolymerChainType.L_PEPTIDE_NH3_AMINO_TERMINUS},
	 * {@link ContainsPolymerChainType#L_PEPTIDE_LINKING ContainsPolymerChainType.L_PEPTIDE_LINKING},
	 * {@link ContainsPolymerChainType#L_SACCHARIDE ContainsPolymerChainType.L_SACCHARIDE},
	 * {@link ContainsPolymerChainType#L_SACCHARIDE_14_AND_14_LINKING ContainsPolymerChainType.L_SACCHARIDE_14_AND_14_LINKING},
	 * {@link ContainsPolymerChainType#L_SACCHARIDE_14_AND_16_LINKING ContainsPolymerChainType.L_SACCHARIDE_14_AND_16_LINKING},
	 * {@link ContainsPolymerChainType#PEPTIDE_LINKING ContainsPolymerChainType.PEPTIDE_LINKING},
	 * {@link ContainsPolymerChainType#RNA_OH_3_PRIME_TERMINUS ContainsPolymerChainType.RNA_OH_3_PRIME_TERMINUS},
	 * {@link ContainsPolymerChainType#RNA_OH_5_PRIME_TERMINUS ContainsPolymerChainType.RNA_OH_5_PRIME_TERMINUS},
	 * {@link ContainsPolymerChainType#OTHER ContainsPolymerChainType.OTHER},
	 * {@link ContainsPolymerChainType#SACCHARIDE ContainsPolymerChainType.SACCHARIDE}
	 */
	public ContainsPolymerChainType(String...monomerTypes) {
        this(false, monomerTypes);
	}
	
	/**
	 * Optional constructor that can be used to filter entries that exclusively match all chains.
	 * @param exclusive if true, all chains must match the specified monomer types.
	 * @param monomerTypes list of monomer types
	 */
	public ContainsPolymerChainType(boolean exclusive, String...monomerType) {
		this.exclusive = exclusive;
		this.entityTypes = new HashSet<>(Arrays.asList(monomerType));
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
