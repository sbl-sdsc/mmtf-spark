package edu.sdsc.mmtf.spark.incubator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.ArrayUtils;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.rcsb.mmtf.api.StructureDataInterface;
import org.rcsb.mmtf.dataholders.Entity;
import org.rcsb.mmtf.decoder.DecoderUtils;
import org.rcsb.mmtf.encoder.AdapterToStructureData;
import org.rcsb.mmtf.encoder.EncoderUtils;

import scala.Tuple2;

/**
 * Maps a PDB entry to its chains.
 * @author Peter Rose
 *
 */
public class StructureToChains implements PairFlatMapFunction<Tuple2<String,StructureDataInterface>,String, StructureDataInterface> {
	private static final long serialVersionUID = -3348372120358649240L;

	@Override
	public Iterator<Tuple2<String, StructureDataInterface>> call(Tuple2<String, StructureDataInterface> t) throws Exception {
		StructureDataInterface structure = t._2;

		int atomCounter= - 1;
		int redAtomCounter = -1;
		int groupCounter= - 1;
		int chainCounter= - 1;
		
		int numChains = structure.getChainsPerModel()[0];
		List<Tuple2<String, StructureDataInterface>> chainList = new ArrayList<>(numChains);
		
		SummaryData[] summaries = getChainSummary(structure);
		int natom = 0;
		for (int i = 0; i < summaries.length; i++) {
			natom += summaries[i].numAtoms;
		}
		
		if (natom != structure.getNumAtoms() && structure.getNumModels() == 1) {
			System.err.println("WARNING: skipping entry: " + structure.getStructureId() + " inconsistency in data structure");
			return chainList.iterator();
		}
			
			for (int j=0; j<numChains; j++){
				chainCounter++;
				String chainType = EncoderUtils.getTypeFromChainId(structure, chainCounter);

				List<Integer> interGroupBondsToAdd = new ArrayList<>();
				List<Integer> interGroupRedIndsToAdd = new ArrayList<>();
				

				AdapterToStructureData adapterToStructureData = new AdapterToStructureData();
				chainList.add(new Tuple2<String, StructureDataInterface>(structure.getStructureId() + "." + structure.getChainIds()[chainCounter], adapterToStructureData));
				SummaryData dataSummary = summaries[j];

//				System.out.println("Chain: " + structureDataInterface.getStructureId() + "-" + structureDataInterface.getChainIds()[j] + ": " + dataSummary);
				adapterToStructureData.initStructure(dataSummary.numBonds, dataSummary.numAtoms, dataSummary.numGroups, 
						dataSummary.numChains, dataSummary.numModels, structure.getStructureId());
				DecoderUtils.addXtalographicInfo(structure, adapterToStructureData);
				DecoderUtils.addHeaderInfo(structure, adapterToStructureData);	
				
				// set model info (only one model: 0)
				adapterToStructureData.setModelInfo(0, dataSummary.numChains);
				
				// set entity info for chain
				Entity entity = getEntityInfo(structure, chainCounter);
//				System.out.println("entity sequence: " + Arrays.toString(entity.getChainIndexList()) + "," + entity.getSequence() + "," + entity.getDescription() + "," + entity.getType());
				adapterToStructureData.setEntityInfo(entity.getChainIndexList(), entity.getSequence(), entity.getDescription(), entity.getType());

				int numGroups=0;
				for(int k=0; k<structure.getGroupsPerChain()[chainCounter]; k++){

					groupCounter++;

					int groupType = structure.getGroupTypeIndices()[groupCounter];
					List<Integer> atomIndicesToAdd = getIndicesToAdd(structure, groupType, chainType);
					int bondsToAdd = findBondsToAdd(atomIndicesToAdd, structure, groupType,atomCounter+1);

						adapterToStructureData.setGroupInfo(structure.getGroupName(groupType), structure.getGroupIds()[groupCounter], 
								structure.getInsCodes()[groupCounter], structure.getGroupChemCompType(groupType), atomIndicesToAdd.size(),
								bondsToAdd, structure.getGroupSingleLetterCode(groupType), structure.getGroupSequenceIndices()[groupCounter], 
								structure.getSecStructList()[groupCounter]);
						numGroups++;

					for(int l=0; l<structure.getNumAtomsInGroup(groupType);l++){
						atomCounter++;
							redAtomCounter++;
							// 1A04: A: 5-216 = 212, b: 5-216: 212
							if (structure.getxCoords().length <= atomCounter) {
								System.out.println("PDB ID: " + structure.getStructureId());
								System.out.println(structure.getGroupName(groupType));
								System.out.println("atom counter: " + atomCounter);
								System.out.println("group counter: " + groupCounter);
								System.out.println("Atom counter too high" + atomCounter + " -> " + structure.getAltLocIds().length);
							}
							adapterToStructureData.setAtomInfo(structure.getGroupAtomNames(groupType)[l], structure.getAtomIds()[atomCounter], structure.getAltLocIds()[atomCounter], 
									structure.getxCoords()[atomCounter], structure.getyCoords()[atomCounter], structure.getzCoords()[atomCounter], 
									structure.getOccupancies()[atomCounter], structure.getbFactors()[atomCounter], structure.getGroupElementNames(groupType)[l], structure.getGroupAtomCharges(groupType)[l]);

							// why this special handing of saccharides?
							if (structure.getGroupChemCompType(groupType).toUpperCase().contains("SACCHARIDE")){
								interGroupBondsToAdd.add(atomCounter);
								interGroupRedIndsToAdd.add(redAtomCounter);
							}
					}
					if(bondsToAdd>0){
						for(int l=0; l<structure.getGroupBondOrders(groupType).length; l++){
							int bondOrder = structure.getGroupBondOrders(groupType)[l];
							int bondIndOne = structure.getGroupBondIndices(groupType)[l*2];
							int bondIndTwo = structure.getGroupBondIndices(groupType)[l*2+1];
							adapterToStructureData.setGroupBond(bondIndOne, bondIndTwo, bondOrder);
						}
					}
				}
				adapterToStructureData.setChainInfo(structure.getChainIds()[chainCounter],
						structure.getChainNames()[chainCounter], numGroups);
				// Add the inter group bonds
				for(int ii=0; ii<structure.getInterGroupBondOrders().length;ii++){
					int bondIndOne = structure.getInterGroupBondIndices()[ii*2];
					int bondIndTwo = structure.getInterGroupBondIndices()[ii*2+1];
					int bondOrder = structure.getInterGroupBondOrders()[ii];
					if(interGroupBondsToAdd.contains(bondIndOne) && interGroupBondsToAdd.contains(bondIndTwo) ){
						int indexOne = interGroupBondsToAdd.indexOf(bondIndOne);
						int indexTwo = interGroupBondsToAdd.indexOf(bondIndTwo);
						adapterToStructureData.setInterGroupBond(interGroupRedIndsToAdd.get(indexOne), interGroupRedIndsToAdd.get(indexTwo), bondOrder);
					}
				}
				adapterToStructureData.finalizeStructure();
		}

		return chainList.iterator();
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
			if (structureDataInterface.getGroupChemCompType(groupType).toUpperCase().contains("SACCHARIDE")){
				for(int i=0; i<structureDataInterface.getGroupBondOrders(groupType).length; i++) {
					if(ArrayUtils.contains(structureDataInterface.getInterGroupBondIndices(),atomCounter+i)){
						interGroupBonds++;
					}
				}
			}
			if(indicesToAdd.size()==structureDataInterface.getNumAtomsInGroup(groupType)){
				return structureDataInterface.getGroupBondOrders(groupType).length+interGroupBonds;
			}
		}
		return 0;
	}

	/**
	 * Get the indices of atoms to add in this group. This is C-alpha, phosphate (DNA) and ligand atoms
	 * @param structureDataInterface the input {@link StructureDataInterface}
	 * @param groupType the index of this group in the groupList
	 * @param chainType the type of the chain (polymer, non-polymer, water).
	 * @return the list of indices (within the group) of atoms to consider
	 */
	private static List<Integer> getIndicesToAdd(StructureDataInterface structureDataInterface, int groupType,
			String chainType) {
		
		List<Integer> outList = new ArrayList<>();
		for (int i = 0; i < structureDataInterface.getNumAtomsInGroup(groupType); i++){
			outList.add(i);
		}
		return outList;
	}
	
	/**
	 * Get the number of bonds, atoms and groups as a map.
	 * @param structureDataInterface the input {@link StructureDataInterface}
	 * @return the {@link SummaryData} object describing the data
	 */
	private static SummaryData[] getChainSummary(StructureDataInterface structureDataInterface) {
		
		int groupCounter = -1;
		int chainCounter=-1;
		int atomCounter = 0;
		
		int numChains = structureDataInterface.getChainsPerModel()[0];
		SummaryData[] summaries = new SummaryData[numChains];
			
			for(int j=0; j<numChains; j++){
				SummaryData summaryData = new SummaryData();
				summaries[j] = summaryData;
				summaryData.numChains = 0;
				summaryData.numGroups = 0;
				summaryData.numAtoms = 0;
				summaryData.numBonds = 0;
				summaryData.numModels = 1;
				
				//TODO add entity info to summary data 
				//			structInflator.setEntityInfo(dataApi.getEntityChainIndexList(i), dataApi.getEntitySequence(i), dataApi.getEntityDescription(i), dataApi.getEntityType(i));

				
				chainCounter++;
				summaryData.numChains++;
				String chainType = EncoderUtils.getTypeFromChainId(structureDataInterface, chainCounter);
				
				for(int k=0; k<structureDataInterface.getGroupsPerChain()[chainCounter]; k++){
					groupCounter++;
					int groupType = structureDataInterface.getGroupTypeIndices()[groupCounter];
					List<Integer> indicesToAdd = getIndicesToAdd(structureDataInterface, groupType, chainType);
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

		return summaries;
	}

	/**
	 * Returns entity information for the chain specified by the chain index.
	 * @param structureDataInterface
	 * @param chainIndex
	 * @return
	 */
	private static Entity getEntityInfo(StructureDataInterface structureDataInterface, int chainIndex) {
		Entity entity = new Entity();
		
		for (int entityInd = 0; entityInd < structureDataInterface.getNumEntities(); entityInd++) {
			
			for (int chainInd: structureDataInterface.getEntityChainIndexList(entityInd)) {
				if (chainInd == chainIndex) {
					entity.setChainIndexList(new int[]{0}); // new chain index is zero, since we extract a single chain
					entity.setDescription(structureDataInterface.getEntityDescription(entityInd));
					entity.setSequence(structureDataInterface.getEntitySequence(entityInd));
					entity.setType(structureDataInterface.getEntityType(entityInd));
					return entity;
				}
			}
		}
		return entity;
	}

}