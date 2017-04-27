package edu.sdsc.mmtf.spark.mappers;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.ArrayUtils;
import org.rcsb.mmtf.api.StructureDataInterface;
import org.rcsb.mmtf.decoder.DecoderUtils;
import org.rcsb.mmtf.encoder.AdapterToStructureData;
import org.rcsb.mmtf.encoder.EncoderUtils;

/**
 * Convert a full format of the file to a reduced format.
 * @author Anthony Bradley
 *
 */
public class ReducedEncoderNew {

	private static final String CALPHA_NAME = "CA";
	private static final String CARBON_ELEMENT = "C";
	private static final String PHOSPHATE_NAME = "P";
	private static final String PHOSPHATE_ELEMENT = "P";

	/**
	 * Get the reduced form of the input {@link StructureDataInterface}.
	 * @param structureDataInterface the input {@link StructureDataInterface} 
	 * @return the reduced form of the {@link StructureDataInterface} as another {@link StructureDataInterface}
	 */
	public static StructureDataInterface getReduced(StructureDataInterface structureDataInterface) {
		Integer[] centerAtomIndices = getCenterAtomGroupIndices(structureDataInterface);
		// The transmission of the data goes through this
		AdapterToStructureData adapterToStructureData = new AdapterToStructureData();
		Map<Integer, Integer> atomMap = new HashMap<>();
		
		SummaryData dataSummary = getDataSummaryData(structureDataInterface, centerAtomIndices);
		adapterToStructureData.initStructure(dataSummary.numBonds, dataSummary.numAtoms, dataSummary.numGroups, 
				dataSummary.numChains, structureDataInterface.getNumModels(), structureDataInterface.getStructureId());
		DecoderUtils.addXtalographicInfo(structureDataInterface, adapterToStructureData);
		DecoderUtils.addHeaderInfo(structureDataInterface, adapterToStructureData);
		DecoderUtils.generateBioAssembly(structureDataInterface, adapterToStructureData);		
		DecoderUtils.addEntityInfo(structureDataInterface, adapterToStructureData);
		// Loop through the Structure data interface this with the appropriate data
		int atomCounter= - 1;
		int redAtomCounter = -1;
		int groupCounter= - 1;
		int chainCounter= - 1;
		List<Integer> interGroupBondsToAdd = new ArrayList<>();
		List<Integer> interGroupRedIndsToAdd = new ArrayList<>();
		for (int i=0; i<structureDataInterface.getNumModels(); i++){
			int numChains = structureDataInterface.getChainsPerModel()[i];
			adapterToStructureData.setModelInfo(i, numChains);
			for(int j=0; j<numChains; j++){
				chainCounter++;
				String chainType = EncoderUtils.getTypeFromChainId(structureDataInterface, chainCounter);
				int numGroups=0;
				for(int k=0; k<structureDataInterface.getGroupsPerChain()[chainCounter]; k++){
					groupCounter++;
					int groupType = structureDataInterface.getGroupTypeIndices()[groupCounter];
					List<Integer> atomIndicesToAdd = getIndicesToAddNew(structureDataInterface, groupType, chainType, centerAtomIndices);
					Set<Integer> atomIndicesToAddSet = new HashSet<>(atomIndicesToAdd);
					int bondsToAdd = findBondsToAdd(atomIndicesToAdd, structureDataInterface, groupType,atomCounter+1);
//					int bondsToAdd =  structureDataInterface.getGroupBondOrders(groupType).length;
					// If there's an atom to add in this group - add it
					if(atomIndicesToAdd.size()>0){
						adapterToStructureData.setGroupInfo(structureDataInterface.getGroupName(groupType), structureDataInterface.getGroupIds()[groupCounter], 
								structureDataInterface.getInsCodes()[groupCounter], structureDataInterface.getGroupChemCompType(groupType), atomIndicesToAddSet.size(),
								bondsToAdd, structureDataInterface.getGroupSingleLetterCode(groupType), structureDataInterface.getGroupSequenceIndices()[groupCounter], 
								structureDataInterface.getSecStructList()[groupCounter]);
						numGroups++;
					}
					for(int l=0; l<structureDataInterface.getNumAtomsInGroup(groupType);l++){
						atomCounter++;
						if(atomIndicesToAddSet.contains(l)){
							redAtomCounter++;
							atomMap.put(atomCounter,  redAtomCounter);
							adapterToStructureData.setAtomInfo(structureDataInterface.getGroupAtomNames(groupType)[l], structureDataInterface.getAtomIds()[atomCounter], structureDataInterface.getAltLocIds()[atomCounter], 
									structureDataInterface.getxCoords()[atomCounter], structureDataInterface.getyCoords()[atomCounter], structureDataInterface.getzCoords()[atomCounter], 
									structureDataInterface.getOccupancies()[atomCounter], structureDataInterface.getbFactors()[atomCounter], structureDataInterface.getGroupElementNames(groupType)[l], structureDataInterface.getGroupAtomCharges(groupType)[l]);
						}
					}
					if(bondsToAdd>0){
						for(int l=0; l<structureDataInterface.getGroupBondOrders(groupType).length; l++){
							int bondOrder = structureDataInterface.getGroupBondOrders(groupType)[l];
							int bondIndOne = structureDataInterface.getGroupBondIndices(groupType)[l*2];
							int bondIndTwo = structureDataInterface.getGroupBondIndices(groupType)[l*2+1];
							adapterToStructureData.setGroupBond(bondIndOne, bondIndTwo, bondOrder);
						}
					}
				}
				adapterToStructureData.setChainInfo(structureDataInterface.getChainIds()[chainCounter],
						structureDataInterface.getChainNames()[chainCounter], numGroups);
			}
		}
		// Add the inter group bonds
		for(int ii=0; ii<structureDataInterface.getInterGroupBondOrders().length;ii++){
			int bondIndOne = structureDataInterface.getInterGroupBondIndices()[ii*2];
			int bondIndTwo = structureDataInterface.getInterGroupBondIndices()[ii*2+1];
			int bondOrder = structureDataInterface.getInterGroupBondOrders()[ii];
			Integer indexOne = atomMap.get(bondIndOne);
			if (indexOne != null) {
				Integer indexTwo = atomMap.get(bondIndTwo);
				if (indexTwo != null) {

					adapterToStructureData.setInterGroupBond(indexOne, indexTwo, bondOrder);
				}
			}
		}
		adapterToStructureData.finalizeStructure();
		// Return the AdapterToStructureData
		return adapterToStructureData;
	}

	/**
	 * Find if bonds need adding - to be used in later processing.
	 * @param indicesToAdd the indices of the atoms to add
	 * @param structureDataInterface the {@link StructureDataInterface} of the total structure
	 * @param groupType the index of the groupType
	 * @param atomCounter the current atom counter position
	 * @return the integer number of bonds to add
	 */
	private static int findBondsToAdd(List<Integer> indicesToAdd, StructureDataInterface structureDataInterface, int groupType, int atomCounter) {
		// Add the bonds if we've copied all the elements
		int interGroupBonds = 0;
		if(indicesToAdd.size()>1){
			// TODO PR is this needed?
//			if (structureDataInterface.getGroupChemCompType(groupType).toUpperCase().contains("SACCHARIDE")){
//				for(int i=0; i<structureDataInterface.getGroupBondOrders(groupType).length; i++) {
//					if(ArrayUtils.contains(structureDataInterface.getInterGroupBondIndices(),atomCounter+i)){
//						interGroupBonds++;
//					}
//				}
//			}
			if(indicesToAdd.size()==structureDataInterface.getNumAtomsInGroup(groupType)){
				return structureDataInterface.getGroupBondOrders(groupType).length+interGroupBonds;
			}
		}
		return 0;
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
					List<Integer> indicesToAdd = getIndicesToAddNew(structureDataInterface, groupType, chainType, centerAtomIndices);
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
					summaryData.numBonds+=findBondsToAdd(indicesToAdd, structureDataInterface, groupType, atomCounter);
				}
			}
		}
		return summaryData;
	}

	/**
	 * Get the indices of atoms to add in this group. This is C-alpha, phosphate (DNA/RNA) and
	 * all non-polymer atoms, except water.
	 * @param structureDataInterface the input {@link StructureDataInterface}
	 * @param groupIndex the index of this group in the groupList
	 * @param chainType the type of the chain (polymer, non-polymer, water).
	 * @return the list of indices (within the group) of atoms to consider
	 */
	private static List<Integer> getIndicesToAdd(StructureDataInterface structureDataInterface, int groupIndex,
			String chainType) {
		// The list to return
		List<Integer> atomIndices = new ArrayList<>();

		// Get chain type
		if(chainType.equals("polymer")){
			
		    // atoms to keep could be calculated once for all unique groups
			Integer atomIndex = getcAlphaIndex(structureDataInterface, groupIndex);
			if (atomIndex != null) {
				// reduce group to cAlpha atom if present
				return Collections.singletonList(atomIndex);
			} else {
				atomIndex = getPhosphateIndex(structureDataInterface, groupIndex);
				if (atomIndex != null) {
					// reduce group to phosphate atom if present
					return Collections.singletonList(atomIndex);
				} else {
					// keep all other polymer atoms, e.g., atoms in saccharides
					for(int i = 0; i<structureDataInterface.getNumAtomsInGroup(groupIndex); i++){
						atomIndices.add(i);
					}
				}
			}
		} else if (chainType.equals("non-polymer") || ! chainType.equals("water")){
			// keep all non-polymer atoms, except water
			for(int i=0; i<structureDataInterface.getNumAtomsInGroup(groupIndex); i++){
				atomIndices.add(i);
			}
		}
		
		return atomIndices;
	}
	
	/**
	 * Get the indices of atoms to add in this group. This is C-alpha, phosphate (DNA/RNA) and
	 * all non-polymer atoms, except water.
	 * @param structureDataInterface the input {@link StructureDataInterface}
	 * @param groupIndex the index of this group in the groupList
	 * @param chainType the type of the chain (polymer, non-polymer, water).
	 * @return the list of indices (within the group) of atoms to consider
	 */
	private static List<Integer> getIndicesToAddNew(StructureDataInterface structureDataInterface, int groupIndex,
			String chainType, Integer[] centerAtomIndices) {
		// The list to return
		List<Integer> atomIndices = Collections.emptyList();

		Integer atomIndex = centerAtomIndices[groupIndex];

		// Get chain type
		if(chainType.equals("polymer")) {
			if (atomIndex != null){
				atomIndices = Collections.singletonList(atomIndex);
			} else {
//				System.out.println(structureDataInterface.getStructureId() + " adding polymer (all) atoms");
				// keep all other polymer atoms, e.g., atoms in saccharides
				atomIndices = new ArrayList<>(structureDataInterface.getNumAtomsInGroup(groupIndex));
				for(int i = 0; i < structureDataInterface.getNumAtomsInGroup(groupIndex); i++) {
					atomIndices.add(i);
				}
			}
		} else if (chainType.equals("non-polymer") || ! chainType.equals("water")){
			// keep all non-polymer atoms, except water
			atomIndices = new ArrayList<>(structureDataInterface.getNumAtomsInGroup(groupIndex));
			for(int i=0; i<structureDataInterface.getNumAtomsInGroup(groupIndex); i++){
				atomIndices.add(i);
			}
		}

		return atomIndices;
	}

	// this info could be calculated once only for each group
    private static Integer getcAlphaIndex(StructureDataInterface structureDataInterface, int groupIndex) {
    	for(int i=0; i<structureDataInterface.getNumAtomsInGroup(groupIndex); i++){
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