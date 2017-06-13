package edu.sdsc.mmtf.spark.incubator;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.rcsb.mmtf.api.StructureDataInterface;
import org.rcsb.mmtf.decoder.DecoderUtils;
import org.rcsb.mmtf.encoder.AdapterToStructureData;
import org.rcsb.mmtf.encoder.EncoderUtils;

/**
 * Converts a full (all-atom) MMTF structure data representation to a reduced version.
 * The reduced version contains only the C-alpha atoms of polypeptide and the
 * P atom of polynucleotide chains. Alternative locations of the C-alpha and P atoms
 * are excluded, as well as any water molecules.
 * 
 * @author Anthony Bradley
 * @author Peter Rose
 *
 */
public class ReducedEncoderNew {

	private static final String CALPHA_NAME = "CA";
	private static final String CARBON_ELEMENT = "C";
	private static final String PHOSPHATE_NAME = "P";
	private static final String PHOSPHATE_ELEMENT = "P";

	/**
	 * Gets the reduced form of the input {@link StructureDataInterface}.
	 * @param full the input {@link StructureDataInterface} 
	 * @return the reduced form of the {@link StructureDataInterface} as another {@link StructureDataInterface}
	 */
	public static StructureDataInterface getReduced(StructureDataInterface full) {
		Integer[] centerAtomIndices = getCenterAtomGroupIndices(full);

		AdapterToStructureData reduced = new AdapterToStructureData();
		Map<Integer, Integer> atomMap = new HashMap<>();
		
		// Set header and metadata
		SummaryData dataSummary = getDataSummaryData(full, centerAtomIndices);
		reduced.initStructure(dataSummary.numBonds, dataSummary.numAtoms, dataSummary.numGroups, 
				dataSummary.numChains, full.getNumModels(), full.getStructureId());
		
		DecoderUtils.addXtalographicInfo(full, reduced);
		DecoderUtils.addHeaderInfo(full, reduced);
		DecoderUtils.generateBioAssembly(full, reduced);		
		DecoderUtils.addEntityInfo(full, reduced);


		// traverse data structure and copy data to reduced representation.
		// Note, atomCount, groupCount, and chainCount keep track of the total number of atoms, groups, and chains.
		// They are required to index the data structure.
		
		for (int i = 0, atomCount = 0, groupCount = 0, chainCount = 0, reducedAtomCount = 0; i<full.getNumModels(); i++) {
			int numChains = full.getChainsPerModel()[i];
			reduced.setModelInfo(i, numChains);
			
			for (int j = 0; j < numChains; j++, chainCount++){
				String chainType = EncoderUtils.getTypeFromChainId(full, chainCount);
				int reducedGroupsPerChain = 0;

				for (int k = 0; k < full.getGroupsPerChain()[chainCount]; k++, groupCount++) {
					int groupType = full.getGroupTypeIndices()[groupCount];
					
					Set<Integer> atomIndicesToAdd = getIndicesToAdd(full, groupType, chainType, centerAtomIndices);
					int bondsToAdd = getNumIntraGroupBonds(atomIndicesToAdd, full, groupType, centerAtomIndices);
					
					if (atomIndicesToAdd.size() > 0) {
						
						// Set Group information
						reduced.setGroupInfo(full.getGroupName(groupType), full.getGroupIds()[groupCount], 
								full.getInsCodes()[groupCount], full.getGroupChemCompType(groupType), atomIndicesToAdd.size(),
								bondsToAdd, full.getGroupSingleLetterCode(groupType), full.getGroupSequenceIndices()[groupCount], 
								full.getSecStructList()[groupCount]);
						
						reducedGroupsPerChain ++;
					}
					
					for (int l = 0; l < full.getNumAtomsInGroup(groupType); l++, atomCount++) {
						if (atomIndicesToAdd.contains(l)) {
							// this atom counter keeps track of the atoms in the reduced structure
							reducedAtomCount++; 
							// this map keeps track of which atoms need to be kept for reduced version
							atomMap.put(atomCount,  reducedAtomCount); 
			
							// Set Atom information
							reduced.setAtomInfo(full.getGroupAtomNames(groupType)[l], full.getAtomIds()[atomCount], full.getAltLocIds()[atomCount], 
									full.getxCoords()[atomCount], full.getyCoords()[atomCount], full.getzCoords()[atomCount], 
									full.getOccupancies()[atomCount], full.getbFactors()[atomCount], full.getGroupElementNames(groupType)[l], full.getGroupAtomCharges(groupType)[l]);
							}
					}

					if (bondsToAdd > 0){
						
						// Set bond information
						for(int l=0; l<full.getGroupBondOrders(groupType).length; l++){
							int index1 = full.getGroupBondIndices(groupType)[l*2];
							int index2 = full.getGroupBondIndices(groupType)[l*2+1];
							int bondOrder = full.getGroupBondOrders(groupType)[l];
							reduced.setGroupBond(index1, index2, bondOrder);
						}
					}
				};
				
				// Set chain information
				reduced.setChainInfo(full.getChainIds()[chainCount],
						full.getChainNames()[chainCount], reducedGroupsPerChain );
			}
		}
		
		addInterGroupBonds(full, reduced, atomMap);
		
		// finalize the data structure
		reduced.finalizeStructure();
		
		return reduced;
	}

	/**
	 * Adds bonds between groups to the reduced data structure.
	 * @param full full representation of structure
	 * @param reduced reduced representation of structure
	 * @param atomMap maps original atom indices to atom indices in the reduced structures
	 */
	private static void addInterGroupBonds(StructureDataInterface full, AdapterToStructureData reduced, Map<Integer, Integer> atomMap) {

		for (int i = 0; i < full.getInterGroupBondOrders().length; i++) {
			int bondIndOne = full.getInterGroupBondIndices()[i*2];
			int bondIndTwo = full.getInterGroupBondIndices()[i*2+1];
			int bondOrder = full.getInterGroupBondOrders()[i];
			
			// some atoms may not exist in the reduced structure. 
			// check the atom map to see if both atoms of a bond still exist.
			Integer indexOne = atomMap.get(bondIndOne);
			
			if (indexOne != null) {
				Integer indexTwo = atomMap.get(bondIndTwo);
				if (indexTwo != null) {
					reduced.setInterGroupBond(indexOne, indexTwo, bondOrder);
				}
			}
		}
	}

	/**
	 * Gets the number of intramolecular bonds for a specified group type.
	 * @param indicesToAdd the indices of the atoms to add
	 * @param structureDataInterface the {@link StructureDataInterface} of the total structure
	 * @param groupType the index of the groupType
	 * @return the integer number of bonds to add
	 */
	private static int getNumIntraGroupBonds(Set<Integer> indicesToAdd, StructureDataInterface structureDataInterface, int groupType, Integer[] centerAtomIndices) {		
	
		if (indicesToAdd.size() == 1 && centerAtomIndices[groupType] != null) {
			// in case there is only 1 atom (c-Alpha or P) and it's in a polymer, there cannot be any bonds
			return 0;
		} else if (indicesToAdd.size() == 0) {
			return 0;
		} else {
			return structureDataInterface.getGroupBondOrders(groupType).length;
		}
	}

	/**
	 * Get the number of bonds, atoms and groups as a map.
	 * @param structureDataInterface the input {@link StructureDataInterface}
	 * @return the {@link SummaryData} object describing the data
	 */
	private static SummaryData getDataSummaryData(StructureDataInterface structureDataInterface, Integer[] centerAtomIndices) {
		SummaryData summaryData = new SummaryData();
		summaryData.numChains = 0;
		summaryData.numGroups = 0;
		summaryData.numAtoms = 0;
		summaryData.numBonds = 0;
	
		for (int i = 0, groupCount = 0, chainCount = 0; i<structureDataInterface.getNumModels(); i++){
			int numChains = structureDataInterface.getChainsPerModel()[i];
			
			for (int j = 0; j < numChains; j++, chainCount++){
				summaryData.numChains++;
				String chainType = EncoderUtils.getTypeFromChainId(structureDataInterface, chainCount);
				
				for (int k = 0; k < structureDataInterface.getGroupsPerChain()[chainCount]; k++, groupCount++){
					int groupType = structureDataInterface.getGroupTypeIndices()[groupCount];
					Set<Integer> indicesToAdd = getIndicesToAdd(structureDataInterface, groupType, chainType, centerAtomIndices);
	
					if (indicesToAdd.size() > 0) {
						summaryData.numGroups++;
					}
					
					for (int l = 0; l < structureDataInterface.getNumAtomsInGroup(groupType); l++){
						if (indicesToAdd.contains(l)) {
							summaryData.numAtoms++;
						}
					}

					summaryData.numBonds+=getNumIntraGroupBonds(indicesToAdd, structureDataInterface, groupType, centerAtomIndices);
				}
			}
		}
		return summaryData;
	}

	/**
	 * Gets the indices of atoms to include in the reduced MMTF representation. 
	 * This is C-alpha, phosphate (DNA/RNA) and all non-polymer atoms, except water.
	 * @param structure the input {@link StructureDataInterface}
	 * @param groupIndex the index of this group in the groupList
	 * @param chainType the type of the chain (polymer, non-polymer, water).
	 * @return the list of indices (within the group) of atoms to consider
	 */
	private static Set<Integer> getIndicesToAdd(StructureDataInterface structure, int groupIndex,
			String chainType, Integer[] centerAtomIndices) {

		Set<Integer> atomIndices = Collections.emptySet();

		Integer atomIndex = centerAtomIndices[groupIndex];

		// Get chain type
		if (chainType.equals("polymer")) {
			if (atomIndex != null){
				// in this case, the atom index points to either the
				// C-alpha or P atom in an amino acid or nucleotide
				atomIndices = Collections.singleton(atomIndex);
			} else {
				// for all other non-standard residues, include all atoms
				atomIndices = new HashSet<>(structure.getNumAtomsInGroup(groupIndex));
				for(int i = 0; i < structure.getNumAtomsInGroup(groupIndex); i++) {
					atomIndices.add(i);
				}
			}
		} else if (! (structure.getGroupName(groupIndex).equals("HOH") 
	          || structure.getGroupName(groupIndex).equals("DOD")) ){
			// Keep all non-polymer atoms, except for water.
			// Water should be of type "water", however, a few structures (1ZY8, 2G10, 2Q44, 2Q40)
			// contain waters as non-polymers. These structures have in common that water has
			// alternative locations. Therefore, we check for "HOH" instead of polymer type water here
			atomIndices = new HashSet<>(structure.getNumAtomsInGroup(groupIndex));
			for (int i = 0; i < structure.getNumAtomsInGroup(groupIndex); i++){
				atomIndices.add(i);
			}
		}

		return atomIndices;
	}

	/**
	 * Returns an index to the position to the C-alpha atom in a group with the specified group index.
	 * @param structureDataInterface
	 * @param groupIndex index of group
	 * @return index of C-alpha atom if present, otherwise null
	 */
    private static Integer indexOfcAlpha(StructureDataInterface structureDataInterface, int groupIndex) {
    	for(int i = 0; i<structureDataInterface.getNumAtomsInGroup(groupIndex); i++){
			String atomName = structureDataInterface.getGroupAtomNames(groupIndex)[i];
			String elementName = structureDataInterface.getGroupElementNames(groupIndex)[i];
			
			if (atomName.equals(CALPHA_NAME) && elementName.equals(CARBON_ELEMENT)) {
				return i;
			}
    	}
    	return null;
    	
    }
    
    /**
     * Returns and index to the position of the P atom in a nucleotide group with the specified group index.
     * @param structureDataInterface
     * @param groupIndex index of group
     * @return index of P atom if present, otherwise null
     */
    private static Integer indexOfpAtom(StructureDataInterface structureDataInterface, int groupIndex) {
    	for(int i=0; i<structureDataInterface.getNumAtomsInGroup(groupIndex); i++){
			String atomName = structureDataInterface.getGroupAtomNames(groupIndex)[i];
			String elementName = structureDataInterface.getGroupElementNames(groupIndex)[i];
			
			if (atomName.equals(PHOSPHATE_NAME) && elementName.equals(PHOSPHATE_ELEMENT)) {
				return i;
			}
    	}
    	return null;
    }

    /**
     * Returns an array of indices to either the c-alpha or phosphate atom position in a group. 
     * Returns a null index if the group doesn't contain a c-alpha or phosphate atom.
     * @param structure
     */
    private static Integer[] getCenterAtomGroupIndices(StructureDataInterface structure) {
    	int maxIndex = 0;

    	for (int i = 0; i < structure.getGroupTypeIndices().length; i++) {
    		maxIndex = Math.max(maxIndex,  structure.getGroupTypeIndices()[i]);
    	}
    	Integer[] centerAtomIndex = new Integer[maxIndex+1];
    	
    	//TODO should only be done for polymer residues. The same residue
    	// could be present in a polymer and as a ligand. This case is
    	// not handled correctly.

    	for (int i = 0; i < maxIndex+1; i++) {
    		Integer index = indexOfcAlpha(structure, i);
    		if (index == null) {
    			index = indexOfpAtom(structure, i);
    		} 
    		// index will be null if it's neither a c-alpha or P atom
    		centerAtomIndex[i] = index;
    	}
    	return centerAtomIndex;
    }

}
