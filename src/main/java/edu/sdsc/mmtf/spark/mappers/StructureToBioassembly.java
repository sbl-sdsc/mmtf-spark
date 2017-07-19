package edu.sdsc.mmtf.spark.mappers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.rcsb.mmtf.api.StructureDataInterface;
import org.rcsb.mmtf.decoder.DecoderUtils;
import org.rcsb.mmtf.encoder.AdapterToStructureData;

import scala.Tuple2;

/**
 * TODO 
 * @author Yue Yu
 */
public class StructureToBioassembly implements PairFlatMapFunction<Tuple2<String,StructureDataInterface>,String, StructureDataInterface> {

	private static final long serialVersionUID = 5878672539252588124L;

	/**
	 * TODO
	 *	 
	 */
	public StructureToBioassembly() {
		
	}
		
	@Override
	public Iterator<Tuple2<String, StructureDataInterface>> call(Tuple2<String, StructureDataInterface> t) throws Exception {
		StructureDataInterface structure = t._2;
		System.out.println("*** BIOASSEMBLY DATA ***");
		System.out.println("Number bioassemblies: " + structure.getNumBioassemblies());
		
		for (int i = 0; i < structure.getNumBioassemblies(); i++) {
			AdapterToStructureData bioAssembly = new AdapterToStructureData();
			String structureId = structure.getStructureId() + "-BioAssembly" + structure.getBioassemblyName(i);
			
			int totAtoms = 0, totBonds = 0, totGroups = 0, totChains = 0, totModels = 0;
			int numTrans = structure.getNumTransInBioassembly(i);
			totModels = structure.getNumModels();
			int[][] bioChainList;
			double[][] transMatrix;
			for(int ii = 0; ii < numTrans; ii++)
			{
				bioChainList[ii] = structure.getChainIndexListForTransform(i, ii);
				transMatrix[ii] = structure.getMatrixForTransform(i, ii);
				for (int j = 0; j < totModels; j++)
				{
					totChains += bioChainList.length;
					for (int k = 0, groupCounter = 0; k < structure.getChainsPerModel()[j]; k++)
					{
						if(Arrays.asList(bioChainList).contains(k))
							totGroups += structure.getGroupsPerChain()[k];
						for (int h = 0; h < structure.getGroupsPerChain()[k]; h++, groupCounter++){
							if(Arrays.asList(bioChainList).contains(k))
							{
								int groupIndex = structure.getGroupTypeIndices()[groupCounter];	
								totAtoms += structure.getNumAtomsInGroup(groupIndex);
								totBonds += structure.getGroupBondOrders(groupIndex).length;
							}
						}
					}
				}
			}
			bioAssembly.initStructure(totBonds * numTrans, totAtoms * numTrans,
					totGroups * numTrans, totChains * numTrans, totModels * numTrans, structureId);
			DecoderUtils.addXtalographicInfo(structure, bioAssembly);
			DecoderUtils.addHeaderInfo(structure, bioAssembly);	
			
			System.out.println("bioassembly: " + structure.getBioassemblyName(i));
			int numTransformations = structure.getNumTransInBioassembly(i);
			System.out.println("  Number transformations: " + numTransformations);
			
			int modelIndex = 0;
			int chainIndex = 0;
			int groupIndex = 0;
			int atomIndex = 0;
			
			// loop through models
			for(int ii = 0; ii < structure.getNumModels(); ii++)
			{
				// precalculate indices
				int numChainsPerModel = structure.getChainsPerModel()[ii] * numTrans;
				bioAssembly.setModelInfo(ii, numChainsPerModel);
				int[] chainToEntityIndex = getChainToEntityIndex(structure);
				//loop through chains
				for(int j = 0; j < structure.getChainsPerModel()[ii]; j++)
				{
					for (int k = 0; k < numTrans; k++)
					{
						int[] currChainList = bioChainList[k];
						double[] currMatrix = transMatrix[k];
					}
					
					int entityToChainIndex = chainToEntityIndex[chainIndex];
					bioAssembly.setEntityInfo(new int[]{chainIndex}, structure.getEntitySequence(entityToChainIndex), 
							structure.getEntityDescription(entityToChainIndex), structure.getEntityType(entityToChainIndex));
					bioAssembly.setChainInfo(structure.getChainIds()[chainIndex], structure.getChainNames()[chainIndex],
							structure.getGroupsPerChain()[chainIndex]);
					chainIndex++;
				} 	
				modelIndex++;
//				for (int j = 0; j < numTransformations; j++) {
//					System.out.println("    transformation: " + j);
//					System.out.println("    chains:         " + Arrays.toString(structure.getChainIndexListForTransform(i, j)));
//					System.out.println("    rotTransMatrix: " + Arrays.toString(structure.getMatrixForTransform(i, j)));
//				}
			}	
			
		}
		
		List<Tuple2<String, StructureDataInterface>> chainList = new ArrayList<>();
		Set<String> seqSet = new HashSet<>();
//		int[] atomsPerChain = new int[numChains];
//		int[] bondsPerChain = new int[numChains];
//		getNumAtomsAndBonds(structure, atomsPerChain, bondsPerChain);
			
		for (int i = 0, atomCounter = 0, groupCounter = 0; i < numChains; i++){	
			AdapterToStructureData polymerChain = new AdapterToStructureData();
			
			int entityToChainIndex = chainToEntityIndex[i];
			boolean polymer = structure.getEntityType(entityToChainIndex).equals("polymer");
			int polymerAtomCount = 0;

			Map<Integer, Integer> atomMap = new HashMap<>();

			if (polymer) {
		        // to avoid of information loss, add chainName/IDs and entity id
				// this required by some queries
				String structureId = structure.getStructureId() + "." + structure.getChainNames()[i] +
						"." + structure.getChainIds()[i] + "." + (entityToChainIndex+1);
				
				// set header
				polymerChain.initStructure(bondsPerChain[i], atomsPerChain[i], 
						structure.getGroupsPerChain()[i], 1, 1, structureId);
				DecoderUtils.addXtalographicInfo(structure, polymerChain);
				DecoderUtils.addHeaderInfo(structure, polymerChain);	

				// set model info (only one model: 0)
				polymerChain.setModelInfo(0, 1);

				// set entity and chain info
				polymerChain.setEntityInfo(new int[]{0}, structure.getEntitySequence(entityToChainIndex), 
						structure.getEntityDescription(entityToChainIndex), structure.getEntityType(entityToChainIndex));
				polymerChain.setChainInfo(structure.getChainIds()[i], structure.getChainNames()[i], structure.getGroupsPerChain()[i]);
			}

			for (int j = 0; j < structure.getGroupsPerChain()[i]; j++, groupCounter++){
				int groupIndex = structure.getGroupTypeIndices()[groupCounter];
				if (polymer) {
					// set group info
					polymerChain.setGroupInfo(structure.getGroupName(groupIndex), structure.getGroupIds()[groupCounter], 
							structure.getInsCodes()[groupCounter], structure.getGroupChemCompType(groupIndex), structure.getNumAtomsInGroup(groupIndex),
							structure.getGroupBondOrders(groupIndex).length, structure.getGroupSingleLetterCode(groupIndex), structure.getGroupSequenceIndices()[groupCounter], 
							structure.getSecStructList()[groupCounter]);
				}

				for (int k = 0; k < structure.getNumAtomsInGroup(groupIndex); k++, atomCounter++){
					if (polymer) {
						// set atom info
						atomMap.put(atomCounter, polymerAtomCount);
						polymerAtomCount++;
						
						polymerChain.setAtomInfo(structure.getGroupAtomNames(groupIndex)[k], structure.getAtomIds()[atomCounter], structure.getAltLocIds()[atomCounter], 
								structure.getxCoords()[atomCounter], structure.getyCoords()[atomCounter], structure.getzCoords()[atomCounter], 
								structure.getOccupancies()[atomCounter], structure.getbFactors()[atomCounter], structure.getGroupElementNames(groupIndex)[k], structure.getGroupAtomCharges(groupIndex)[k]);
					}
				}

				if (polymer) {
					// add intra-group bond info
					for (int l = 0; l < structure.getGroupBondOrders(groupIndex).length; l++) {
						int bondIndOne = structure.getGroupBondIndices(groupIndex)[l*2];
						int bondIndTwo = structure.getGroupBondIndices(groupIndex)[l*2+1];
						int bondOrder = structure.getGroupBondOrders(groupIndex)[l];
						polymerChain.setGroupBond(bondIndOne, bondIndTwo, bondOrder);
					}
				}
			}

			if (polymer) {
				// Add inter-group bond info
				for(int ii = 0; ii < structure.getInterGroupBondOrders().length; ii++){
					int bondIndOne = structure.getInterGroupBondIndices()[ii*2];
					int bondIndTwo = structure.getInterGroupBondIndices()[ii*2+1];
					int bondOrder = structure.getInterGroupBondOrders()[ii];
					Integer indexOne = atomMap.get(bondIndOne);
					if (indexOne != null) {
						Integer indexTwo = atomMap.get(bondIndTwo);
						if (indexTwo != null) {
							polymerChain.setInterGroupBond(indexOne, indexTwo, bondOrder);
						}
					}
				}

				polymerChain.finalizeStructure();
				
				String chId = structure.getChainNames()[i];
				
				chainList.add(new Tuple2<String, StructureDataInterface>(structure.getStructureId() + "." + chId, polymerChain));
			}
		}

		return chainList.iterator();
	}

	/**
	 * Gets the number of atoms and bonds per chain.
	 */
	private static void getNumAtomsAndBonds(StructureDataInterface structure, int[] atomsPerChain, int[] bondsPerChain) {
		int numChains = structure.getChainsPerModel()[0];

		for (int i = 0, groupCounter = 0; i < numChains; i++){	
			for (int j = 0; j < structure.getGroupsPerChain()[i]; j++, groupCounter++){
				int groupIndex = structure.getGroupTypeIndices()[groupCounter];
				atomsPerChain[i] += structure.getNumAtomsInGroup(groupIndex);
				bondsPerChain[i] += structure.getGroupBondOrders(groupIndex).length;
			}
		}
	}

	/**
	 * Returns an array that maps a chain index to an entity index.
	 * @param structureDataInterface
	 * @return
	 */
	private static int[] getChainToEntityIndex(StructureDataInterface structure) {
		int[] entityChainIndex = new int[structure.getNumChains()];

		for (int i = 0; i < structure.getNumEntities(); i++) {
			for (int j: structure.getEntityChainIndexList(i)) {
				entityChainIndex[j] = i;
			}
		}
		return entityChainIndex;
	}
}