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

import scala.Tuple2;

/**
 * Convert a full format of the file to a reduced format.
 * @author Anthony Bradley
 *
 */
public class StructureToNonPolymerChains implements PairFlatMapFunction<Tuple2<String,StructureDataInterface>,String, StructureDataInterface> {
	private static final long serialVersionUID = -3348372120358649240L;

	@Override
	public Iterator<Tuple2<String, StructureDataInterface>> call(Tuple2<String, StructureDataInterface> t) throws Exception {
		return getReduced(t._2).iterator();
	}


	/**
	 * Get the reduced form of the input {@link StructureDataInterface}.
	 * @param structureDataInterface the input {@link StructureDataInterface} 
	 * @return the reduced form of the {@link StructureDataInterface} as another {@link StructureDataInterface}
	 */
	public static List<Tuple2<String, StructureDataInterface>> getReduced(StructureDataInterface structureDataInterface) {
		// The transmission of the data goes through this

		// Loop through the Structure data interface this with the appropriate data
		int atomCounter= - 1;
		int groupCounter= - 1;
		int chainCounter= - 1;

		int iBond = 0;

		// check what other data are missing: e.g., index: seq. -> atom records
		if (structureDataInterface.getxCoords().length != structureDataInterface.getNumAtoms() && structureDataInterface.getNumModels() == 1) {
			System.out.println("Size mismatch: " + structureDataInterface.getNumAtoms() + " vs " + structureDataInterface.getxCoords().length);
		}

		int numChains = structureDataInterface.getChainsPerModel()[0];
		List<Tuple2<String, StructureDataInterface>> chainList = new ArrayList<>();

		int natom = 0;
		SummaryData[] summaries = getChainSummary(structureDataInterface);
		for (int t = 0; t < summaries.length; t++) {
//			System.out.println(summaries[t]);
			natom += summaries[t].numAtoms;
		}

		//       System.out.println(structureDataInterface.getStructureId() + " Inter bonds: " + structureDataInterface.getInterGroupBondIndices().length);
//		if (natom > structureDataInterface.getNumAtoms() && structureDataInterface.getNumModels() == 1) {
//			System.err.println("WARNING: skipping entry: " + structureDataInterface.getStructureId() + " inconsistency in data structure");
//					System.err.println("Total atoms: " + structureDataInterface.getNumAtoms());
//					System.err.println("Total atoms in groups: " + natom);
//			return chainList;
//		}

		for (int j=0; j<numChains; j++){	
			chainCounter++;
			int currGroupCounter = -1;
			int currAtomCounter = -1;
			boolean inconsistency = false;

			Map<Integer, Integer> atomMap = new HashMap<>();

			Entity entity = getEntityInfo(structureDataInterface, j);
//			System.out.println("Entity info: " + entity.getType() + ", " + entity.getDescription());
			boolean nonPolymer = entity.getType().equals("non-polymer") && ! entity.getType().equals("water");

			AdapterToStructureData adapterToStructureData = new AdapterToStructureData();

			if (nonPolymer) {
				
				SummaryData dataSummary = summaries[j];

				//				System.out.println("Chain: " + structureDataInterface.getChainIds()[j] + ": " + dataSummary);
				adapterToStructureData.initStructure(dataSummary.numBonds, dataSummary.numAtoms, dataSummary.numGroups, 
						dataSummary.numChains, dataSummary.numModels, structureDataInterface.getStructureId());

				DecoderUtils.addXtalographicInfo(structureDataInterface, adapterToStructureData);
				DecoderUtils.addHeaderInfo(structureDataInterface, adapterToStructureData);	

				// set model info (only one model: 0)
				adapterToStructureData.setModelInfo(0, numChains);
//			    System.out.println("entity sequence: " + Arrays.toString(entity.getChainIndexList()) + "," + entity.getSequence() + "," + entity.getDescription() + "," + entity.getType());
				// new to update chainIndexList
				adapterToStructureData.setEntityInfo(entity.getChainIndexList(), entity.getSequence(), entity.getDescription(), entity.getType());
			}


			for(int k=0; k<structureDataInterface.getGroupsPerChain()[chainCounter]; k++){
				groupCounter++;
				int groupIndex = structureDataInterface.getGroupTypeIndices()[groupCounter];

				if (nonPolymer) {
					currGroupCounter++;
					adapterToStructureData.setGroupInfo(structureDataInterface.getGroupName(groupIndex), structureDataInterface.getGroupIds()[groupCounter], 
							structureDataInterface.getInsCodes()[groupCounter], structureDataInterface.getGroupChemCompType(groupIndex), structureDataInterface.getNumAtomsInGroup(groupIndex),
							structureDataInterface.getGroupBondOrders(groupIndex).length, structureDataInterface.getGroupSingleLetterCode(groupIndex), structureDataInterface.getGroupSequenceIndices()[groupCounter], 
							structureDataInterface.getSecStructList()[groupCounter]);
				}

				for (int l=0; l<structureDataInterface.getNumAtomsInGroup(groupIndex); l++){
					atomCounter++;
//					if (currAtomCounter+1 > adapterToStructureData.getNumAtoms()) {
//						System.out.println(structureDataInterface.getStructureId() + " skipping entry wiht atom inconsistency ");
//						chainList.clear();
//						return chainList;
//					}

//					// 1A04: A: 5-216 = 212, b: 5-216: 212, 1GIZ: 43 atoms proble: ORP in DNA seq.type: SACCHARIDE?
//					if (structureDataInterface.getxCoords().length <= atomCounter) {
//						System.out.println("PDB ID: " + structureDataInterface.getStructureId());
//						System.out.println("MMTF V: " + structureDataInterface.getMmtfVersion());
//						System.out.println("Chain: " + structureDataInterface.getChainIds()[j] + ": " + summaries[j]);
//						System.out.println("Num atoms in group: " + structureDataInterface.getNumAtomsInGroup(groupIndex));
//
//						System.out.println(structureDataInterface.getGroupName(groupIndex));
//						System.out.println("atom counter: " + atomCounter);
//						System.out.println("group counter: " + groupCounter);
//						System.out.println("Atom counter too high" + atomCounter + " -> " + structureDataInterface.getAltLocIds().length);
//					}
					// this condition is only true for reduced version, how to determine this?
//					if (polymer && l == 1) {
//						inconsistency = true;
//						System.out.println("Polymer with more than 1 reduced atom");
//						System.out.println("Num atoms in group: " + structureDataInterface.getNumAtomsInGroup(groupIndex));
//						System.out.println("group name " + structureDataInterface.getGroupName(groupIndex));
//						System.out.println("Entity info: " + entity.getType() + ", " + entity.getDescription());
//					}
					if (nonPolymer) {
						currAtomCounter++;
						atomMap.put(atomCounter,  currAtomCounter);
						try {
							adapterToStructureData.setAtomInfo(structureDataInterface.getGroupAtomNames(groupIndex)[l], structureDataInterface.getAtomIds()[atomCounter], structureDataInterface.getAltLocIds()[atomCounter], 
									structureDataInterface.getxCoords()[atomCounter], structureDataInterface.getyCoords()[atomCounter], structureDataInterface.getzCoords()[atomCounter], 
									structureDataInterface.getOccupancies()[atomCounter], structureDataInterface.getbFactors()[atomCounter], structureDataInterface.getGroupElementNames(groupIndex)[l], structureDataInterface.getGroupAtomCharges(groupIndex)[l]);
						} catch (Exception e) {
							System.err.println(structureDataInterface.getStructureId() + ": skipping entry with inconsistent data");
							chainList.clear();
							return chainList;
						}
					}
				}

				if (nonPolymer) {
					for(int l=0; l<structureDataInterface.getGroupBondOrders(groupIndex).length; l++){
						int bondOrder = structureDataInterface.getGroupBondOrders(groupIndex)[l];
						int bondIndOne = structureDataInterface.getGroupBondIndices(groupIndex)[l*2];
						int bondIndTwo = structureDataInterface.getGroupBondIndices(groupIndex)[l*2+1];
						adapterToStructureData.setGroupBond(bondIndOne, bondIndTwo, bondOrder);
					}
				}
			}

			if (nonPolymer) {
				adapterToStructureData.setChainInfo(structureDataInterface.getChainIds()[chainCounter],
						structureDataInterface.getChainNames()[chainCounter], currGroupCounter);
				
				// Add the inter group bonds
				for(int ii=0; ii<structureDataInterface.getInterGroupBondOrders().length;ii++){
					int bondIndOne = structureDataInterface.getInterGroupBondIndices()[ii*2];
					int bondIndTwo = structureDataInterface.getInterGroupBondIndices()[ii*2+1];
					int bondOrder = structureDataInterface.getInterGroupBondOrders()[ii];
					Integer indexOne = atomMap.get(bondIndOne);
					if (indexOne != null) {
						Integer indexTwo = atomMap.get(bondIndTwo);
						if (indexTwo != null) {
							iBond++;
							adapterToStructureData.setInterGroupBond(indexOne, indexTwo, bondOrder);
						}
					}
				}
//				if (iBond > 0) {
//					System.out.println("Intrabond added: " + iBond);
//				}
				adapterToStructureData.finalizeStructure();
				if (currGroupCounter+1 != adapterToStructureData.getNumGroups()) {
					System.out.println("Group inconsistency: " + currGroupCounter + " -> " + adapterToStructureData.getNumGroups());
				}
				if (currAtomCounter+1 != adapterToStructureData.getNumAtoms()) {
					System.out.println("Atom inconsistency: " + currAtomCounter + " -> " + adapterToStructureData.getNumAtoms());
				}
				chainList.add(new Tuple2<String, StructureDataInterface>(structureDataInterface.getStructureId() + "." + structureDataInterface.getChainIds()[chainCounter], adapterToStructureData));
			}
		}

//		if (structureDataInterface.getInterGroupBondOrders().length != iBond) {
//			System.out.println("intrabond missmatch: " + iBond + " out of " + structureDataInterface.getInterGroupBondOrders().length);
//		}

		return chainList;
	}

	/**
	 * Get the number of bonds, atoms and groups as a map.
	 * @param structureDataInterface the input {@link StructureDataInterface}
	 * @return the {@link SummaryData} object describing the data
	 */
	private static SummaryData[] getChainSummary(StructureDataInterface structureDataInterface) {
		int groupCounter = -1;
		int chainCounter=-1;

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

			chainCounter++;
			Entity entity = getEntityInfo(structureDataInterface, chainCounter);
			boolean polymer = entity.getType().equals("polymer");
			
			summaryData.numChains++;

			for(int k=0; k<structureDataInterface.getGroupsPerChain()[chainCounter]; k++){
				groupCounter++;
				int groupIndex = structureDataInterface.getGroupTypeIndices()[groupCounter];

				if (polymer) {
					summaryData.numGroups++;
					summaryData.numAtoms += structureDataInterface.getNumAtomsInGroup(groupIndex);
				    summaryData.numBonds += structureDataInterface.getGroupBondOrders(groupIndex).length;
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