package edu.sdsc.mmtf.spark.mappers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.ArrayUtils;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.rcsb.mmtf.api.StructureDataInterface;
import org.rcsb.mmtf.dataholders.Entity;
import org.rcsb.mmtf.decoder.DecoderUtils;
import org.rcsb.mmtf.encoder.AdapterToStructureData;
import org.rcsb.mmtf.encoder.EncoderUtils;

import edu.sdsc.mmtf.spark.incubator.SummaryData;
import scala.Tuple2;

/**
 * Convert a full format of the file to a reduced format.
 * @author Anthony Bradley
 *
 */
public class StructureToPolymerChains implements PairFlatMapFunction<Tuple2<String,StructureDataInterface>,String, StructureDataInterface> {
	private static final long serialVersionUID = -3348372120358649240L;

	@Override
	public Iterator<Tuple2<String, StructureDataInterface>> call(Tuple2<String, StructureDataInterface> t) throws Exception {
		return getReduced(t._2).iterator();
	}


	/**
	 * Get the reduced form of the input {@link StructureDataInterface}.
	 * @param structure the input {@link StructureDataInterface} 
	 * @return the reduced form of the {@link StructureDataInterface} as another {@link StructureDataInterface}
	 */
	public static List<Tuple2<String, StructureDataInterface>> getReduced(StructureDataInterface structure) {
		// check what other data are missing: e.g., index: seq. -> atom records
		if (structure.getxCoords().length != structure.getNumAtoms() && structure.getNumModels() == 1) {
			System.out.println("Size mismatch: " + structure.getNumAtoms() + " vs " + structure.getxCoords().length);
		}

		int numChains = structure.getChainsPerModel()[0];
		List<Tuple2<String, StructureDataInterface>> chainList = new ArrayList<>();

		SummaryData[] summaries = getChainSummary(structure);

		for (int i = 0, atomCounter = 0, groupCounter = 0; i < numChains; i++){	
			int currGroupCounter = -1;
			int currAtomCounter = -1;

			Map<Integer, Integer> atomMap = new HashMap<>();

			Entity entity = getEntityInfo(structure, i);
//			System.out.println("Entity info: " + entity.getType() + ", " + entity.getDescription());
			boolean polymer = entity.getType().equals("polymer");

			AdapterToStructureData adapterToStructureData = new AdapterToStructureData();

			if (polymer) {
				
				SummaryData dataSummary = summaries[i];

//				System.out.println("Chain symmary : " + structure.getChainIds()[i] + ": " + dataSummary);
				adapterToStructureData.initStructure(dataSummary.numBonds, dataSummary.numAtoms, dataSummary.numGroups, 
						1, 1, structure.getStructureId());

				DecoderUtils.addXtalographicInfo(structure, adapterToStructureData);
				DecoderUtils.addHeaderInfo(structure, adapterToStructureData);	

				// set model info (only one model: 0)
				adapterToStructureData.setModelInfo(0, 1);
//			    System.out.println("entity sequence: " + Arrays.toString(entity.getChainIndexList()) + "," + entity.getSequence() + "," + entity.getDescription() + "," + entity.getType());
				// new to update chainIndexList
				adapterToStructureData.setEntityInfo(entity.getChainIndexList(), entity.getSequence(), entity.getDescription(), entity.getType());
			    adapterToStructureData.setChainInfo(structure.getChainIds()[i], structure.getChainNames()[i], dataSummary.numGroups);
			}
			
//			System.out.println("groups per chain " + i + ": " + structure.getGroupsPerChain()[i]);

			for (int j = 0; j < structure.getGroupsPerChain()[i]; j++, groupCounter++){
				int groupIndex = structure.getGroupTypeIndices()[groupCounter];
				if (polymer) {
					currGroupCounter++;
//					System.out.println("StructureToPolymerChain: " + structure.getGroupChemCompType(groupIndex));
					adapterToStructureData.setGroupInfo(structure.getGroupName(groupIndex), structure.getGroupIds()[groupCounter], 
							structure.getInsCodes()[groupCounter], structure.getGroupChemCompType(groupIndex), structure.getNumAtomsInGroup(groupIndex),
							structure.getGroupBondOrders(groupIndex).length, structure.getGroupSingleLetterCode(groupIndex), structure.getGroupSequenceIndices()[groupCounter], 
							structure.getSecStructList()[groupCounter]);
				}

				for (int k = 0; k < structure.getNumAtomsInGroup(groupIndex); k++, atomCounter++){
					if (polymer) {
						currAtomCounter++;
						atomMap.put(atomCounter,  currAtomCounter);
						try {
							adapterToStructureData.setAtomInfo(structure.getGroupAtomNames(groupIndex)[k], structure.getAtomIds()[atomCounter], structure.getAltLocIds()[atomCounter], 
									structure.getxCoords()[atomCounter], structure.getyCoords()[atomCounter], structure.getzCoords()[atomCounter], 
									structure.getOccupancies()[atomCounter], structure.getbFactors()[atomCounter], structure.getGroupElementNames(groupIndex)[k], structure.getGroupAtomCharges(groupIndex)[k]);
						} catch (Exception e) {
							System.err.println(structure.getStructureId() + ": skipping entry with inconsistent data");
							chainList.clear();
							return chainList;
						}
					}
				}

				if (polymer) {
					for(int l=0; l<structure.getGroupBondOrders(groupIndex).length; l++){
						int bondOrder = structure.getGroupBondOrders(groupIndex)[l];
						int bondIndOne = structure.getGroupBondIndices(groupIndex)[l*2];
						int bondIndTwo = structure.getGroupBondIndices(groupIndex)[l*2+1];
						adapterToStructureData.setGroupBond(bondIndOne, bondIndTwo, bondOrder);
					}
				}
			}

			if (polymer) {
				
				// Add the inter group bonds
				for(int ii=0; ii<structure.getInterGroupBondOrders().length;ii++){
					int bondIndOne = structure.getInterGroupBondIndices()[ii*2];
					int bondIndTwo = structure.getInterGroupBondIndices()[ii*2+1];
					int bondOrder = structure.getInterGroupBondOrders()[ii];
					Integer indexOne = atomMap.get(bondIndOne);
					if (indexOne != null) {
						Integer indexTwo = atomMap.get(bondIndTwo);
						if (indexTwo != null) {
							adapterToStructureData.setInterGroupBond(indexOne, indexTwo, bondOrder);
						}
					}
				}
//				if (iBond > 0) {
//					System.out.println("Intrabond added: " + iBond);
//				}
				adapterToStructureData.finalizeStructure();
//				System.out.println("finalizing chain: polymer");
				if (currGroupCounter+1 != adapterToStructureData.getNumGroups()) {
					System.out.println("Group inconsistency: " + currGroupCounter + " -> " + adapterToStructureData.getNumGroups());
				}
				if (currAtomCounter+1 != adapterToStructureData.getNumAtoms()) {
					System.out.println("Atom inconsistency: " + currAtomCounter + " -> " + adapterToStructureData.getNumAtoms());
				}
//				System.out.println("# groups: " + adapterToStructureData.getNumGroups());
//				System.out.println("# groups: " + adapterToStructureData.getGroupsPerChain()[0]);
				chainList.add(new Tuple2<String, StructureDataInterface>(structure.getStructureId() + "." + structure.getChainIds()[i], adapterToStructureData));
			}
		}

//		if (structureDataInterface.getInterGroupBondOrders().length != iBond) {
//			System.out.println("intrabond missmatch: " + iBond + " out of " + structureDataInterface.getInterGroupBondOrders().length);
//		}

//		System.out.println("chainList:  " + chainList.size());
		return chainList;
	}

	/**
	 * Get the number of bonds, atoms and groups as a map.
	 * @param structure the input {@link StructureDataInterface}
	 * @return the {@link SummaryData} object describing the data
	 */
	private static SummaryData[] getChainSummary(StructureDataInterface structure) {
		int numChains = structure.getChainsPerModel()[0];
		SummaryData[] summaries = new SummaryData[numChains];

		for (int i = 0, groupCounter = 0; i < numChains; i++){	
			SummaryData summaryData = new SummaryData();
			summaries[i] = summaryData;
			
			summaryData.numChains = 1;
			summaryData.numGroups = structure.getGroupsPerChain()[i];
			summaryData.numAtoms = 0;
			summaryData.numBonds = 0;
			summaryData.numModels = 1;
		
			Entity entity = getEntityInfo(structure, i);
			boolean polymer = entity.getType().equals("polymer");
//			System.out.println("Chain: " + i + " polymer: " + polymer);

			for (int j = 0; j < structure.getGroupsPerChain()[i]; j++, groupCounter++){
				if (polymer) {
					int groupIndex = structure.getGroupTypeIndices()[groupCounter];
					summaryData.numAtoms += structure.getNumAtomsInGroup(groupIndex);
				    summaryData.numBonds += structure.getGroupBondOrders(groupIndex).length;
				}
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