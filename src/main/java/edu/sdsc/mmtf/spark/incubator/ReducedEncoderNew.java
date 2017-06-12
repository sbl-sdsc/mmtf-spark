package edu.sdsc.mmtf.spark.incubator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.rcsb.mmtf.api.StructureDataInterface;
import org.rcsb.mmtf.decoder.DecoderUtils;
import org.rcsb.mmtf.encoder.AdapterToStructureData;
import org.rcsb.mmtf.encoder.EncoderUtils;

/**
 * Convert a full format of the file to a reduced format.
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
	 * Get the reduced form of the input {@link StructureDataInterface}.
	 * @param full the input {@link StructureDataInterface} 
	 * @return the reduced form of the {@link StructureDataInterface} as another {@link StructureDataInterface}
	 */
	public static StructureDataInterface getReduced(StructureDataInterface full) {
		Integer[] centerAtomIndices = getCenterAtomGroupIndices(full);
		// The transmission of the data goes through this
		AdapterToStructureData reduced = new AdapterToStructureData();
		Map<Integer, Integer> atomMap = new HashMap<>();
		
		SummaryData dataSummary = getDataSummaryData(full, centerAtomIndices);
		reduced.initStructure(dataSummary.numBonds, dataSummary.numAtoms, dataSummary.numGroups, 
				dataSummary.numChains, full.getNumModels(), full.getStructureId());
		DecoderUtils.addXtalographicInfo(full, reduced);
		DecoderUtils.addHeaderInfo(full, reduced);
		DecoderUtils.generateBioAssembly(full, reduced);		
		DecoderUtils.addEntityInfo(full, reduced);
		// Loop through the Structure data interface this with the appropriate data
		int atomCounter= - 1;
		int redAtomCounter = -1;
		int groupCounter= - 1;
		int chainCounter= - 1;

		for (int i=0; i<full.getNumModels(); i++){
			int numChains = full.getChainsPerModel()[i];
			reduced.setModelInfo(i, numChains);
			for(int j=0; j<numChains; j++){
				chainCounter++;
				// TODO create array of entity types only once, see .. ContainsPolymerChain
				String chainType = EncoderUtils.getTypeFromChainId(full, chainCounter);
				int numGroups=0;
				for(int k=0; k<full.getGroupsPerChain()[chainCounter]; k++){
					groupCounter++;
					int groupType = full.getGroupTypeIndices()[groupCounter];
					
					// TODO need only the set, not list!
					List<Integer> atomIndicesToAdd = getIndicesToAdd(full, groupType, chainType, centerAtomIndices);
					Set<Integer> atomIndicesToAddSet = new HashSet<>(atomIndicesToAdd);
					int bondsToAdd = getNumIntraBonds(atomIndicesToAdd, full, groupType,atomCounter+1, centerAtomIndices);
					// If there's an atom to add in this group - add it
					if (atomIndicesToAdd.size() > 0) {
						reduced.setGroupInfo(full.getGroupName(groupType), full.getGroupIds()[groupCounter], 
								full.getInsCodes()[groupCounter], full.getGroupChemCompType(groupType), atomIndicesToAddSet.size(),
								bondsToAdd, full.getGroupSingleLetterCode(groupType), full.getGroupSequenceIndices()[groupCounter], 
								full.getSecStructList()[groupCounter]);
						numGroups++;
					}
					for(int l=0; l<full.getNumAtomsInGroup(groupType);l++){
						atomCounter++;
						if(atomIndicesToAddSet.contains(l)){
							redAtomCounter++;
							atomMap.put(atomCounter,  redAtomCounter);
			//				try {
							reduced.setAtomInfo(full.getGroupAtomNames(groupType)[l], full.getAtomIds()[atomCounter], full.getAltLocIds()[atomCounter], 
									full.getxCoords()[atomCounter], full.getyCoords()[atomCounter], full.getzCoords()[atomCounter], 
									full.getOccupancies()[atomCounter], full.getbFactors()[atomCounter], full.getGroupElementNames(groupType)[l], full.getGroupAtomCharges(groupType)[l]);
//							} catch (Exception e) {
//								System.out.println("Error: " + full.getStructureId());
//								System.exit(-1);
//							}
							}
					}
					// TODO check below if the bonds should be added
					// check if atoms are still in group
					if (bondsToAdd > 0){
//						System.out.println("Adding Bonds for : " + chainType + ": " + structureDataInterface.getGroupName(groupType) + ": " + bondsToAdd);
						for(int l=0; l<full.getGroupBondOrders(groupType).length; l++){
							int index1 = full.getGroupBondIndices(groupType)[l*2];
							int index2 = full.getGroupBondIndices(groupType)[l*2+1];
							int bondOrder = full.getGroupBondOrders(groupType)[l];
							reduced.setGroupBond(index1, index2, bondOrder);
						}
					}
				}
				reduced.setChainInfo(full.getChainIds()[chainCounter],
						full.getChainNames()[chainCounter], numGroups);
			}
		}
		// Add the inter group bonds
		for(int ii=0; ii<full.getInterGroupBondOrders().length;ii++){
			int bondIndOne = full.getInterGroupBondIndices()[ii*2];
			int bondIndTwo = full.getInterGroupBondIndices()[ii*2+1];
			int bondOrder = full.getInterGroupBondOrders()[ii];
			Integer indexOne = atomMap.get(bondIndOne);
			if (indexOne != null) {
				Integer indexTwo = atomMap.get(bondIndTwo);
				if (indexTwo != null) {

					reduced.setInterGroupBond(indexOne, indexTwo, bondOrder);
				}
			}
		}
		reduced.finalizeStructure();
		// Return the AdapterToStructureData
		return reduced;
	}

	/**
	 * Find if bonds need adding - to be used in later processing.
	 * @param indicesToAdd the indices of the atoms to add
	 * @param structureDataInterface the {@link StructureDataInterface} of the total structure
	 * @param groupType the index of the groupType
	 * @param atomCounter the current atom counter position
	 * @return the integer number of bonds to add
	 */
	private static int getNumIntraBonds(List<Integer> indicesToAdd, StructureDataInterface structureDataInterface, int groupType, int atomCounter, Integer[] centerAtomIndices) {		
	
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
		int groupCounter = -1;
		int chainCounter=-1;
		int atomCounter = 0;
		
		for (int i=0; i<structureDataInterface.getNumModels(); i++){
			int numChains = structureDataInterface.getChainsPerModel()[i];
			
			for(int j=0; j<numChains; j++){
				chainCounter++;
				summaryData.numChains++;
				String chainType = EncoderUtils.getTypeFromChainId(structureDataInterface, chainCounter);
				
				for(int k=0; k<structureDataInterface.getGroupsPerChain()[chainCounter]; k++){
					groupCounter++;
					int groupType = structureDataInterface.getGroupTypeIndices()[groupCounter];
					List<Integer> indicesToAdd = getIndicesToAdd(structureDataInterface, groupType, chainType, centerAtomIndices);
					// If there's an atom to add in this group - add it
					if(indicesToAdd.size()>0){
						summaryData.numGroups++;
					}
					for(int l=0; l<structureDataInterface.getNumAtomsInGroup(groupType);l++){
						if(indicesToAdd.contains(l)){
							summaryData.numAtoms++;
						}
						atomCounter++;
					}
					// Add the bonds if we've copied all the elements
					summaryData.numBonds+=getNumIntraBonds(indicesToAdd, structureDataInterface, groupType, atomCounter, centerAtomIndices);
				}
			}
		}
		return summaryData;
	}

	/**
	 * Get the indices of atoms to include in the reduced MMTF representation. 
	 * This is C-alpha, phosphate (DNA/RNA) and
	 * all non-polymer atoms, except water.
	 * @param structure the input {@link StructureDataInterface}
	 * @param groupIndex the index of this group in the groupList
	 * @param chainType the type of the chain (polymer, non-polymer, water).
	 * @return the list of indices (within the group) of atoms to consider
	 */
	private static List<Integer> getIndicesToAdd(StructureDataInterface structure, int groupIndex,
			String chainType, Integer[] centerAtomIndices) {

		List<Integer> atomIndices = Collections.emptyList();

		Integer atomIndex = centerAtomIndices[groupIndex];

		// Get chain type
		if (chainType.equals("polymer")) {
			if (atomIndex != null){
				// in this case, the atom index points to either the
				// C-alpha or P atom in an amino acid or nucleotide
				atomIndices = Collections.singletonList(atomIndex);
			} else {
				// for all other non-standard residues, include all atoms
				atomIndices = new ArrayList<>(structure.getNumAtomsInGroup(groupIndex));
				for(int i = 0; i < structure.getNumAtomsInGroup(groupIndex); i++) {
					atomIndices.add(i);
				}
			}
		} else if (! structure.getGroupName(groupIndex).equals("HOH")){
			// Keep all non-polymer atoms, except for water.
			// Water should be of type "water", however, a few structures (1ZY8, 2G10, 2Q44, 2Q40)
			// contain waters as non-polymers. These structures have in common that water has
			// alternative locations. Therefore, we check for "HOH" instead of polymer type water here
			atomIndices = new ArrayList<>(structure.getNumAtomsInGroup(groupIndex));
			for(int i=0; i<structure.getNumAtomsInGroup(groupIndex); i++){
				atomIndices.add(i);
			}
		}

		return atomIndices;
	}

    private static Integer getcAlphaIndex(StructureDataInterface structureDataInterface, int groupIndex) {
    	for(int i = 0; i<structureDataInterface.getNumAtomsInGroup(groupIndex); i++){
			String atomName = structureDataInterface.getGroupAtomNames(groupIndex)[i];
			String elementName = structureDataInterface.getGroupElementNames(groupIndex)[i];
			if (atomName.equals(CALPHA_NAME) && elementName.equals(CARBON_ELEMENT)) {
				return i;
			}
    	}
    	return null;
    	
    }
    
    private static Integer getPhosphateIndex(StructureDataInterface structureDataInterface, int groupIndex) {
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
    		Integer index = getcAlphaIndex(structure, i);
    		if (index == null) {
    			index = getPhosphateIndex(structure, i);
    		} 
    		// index will be null if it's neither a c-alpha or P atom
    		centerAtomIndex[i] = index;
    	}
    	return centerAtomIndex;
    }

}
