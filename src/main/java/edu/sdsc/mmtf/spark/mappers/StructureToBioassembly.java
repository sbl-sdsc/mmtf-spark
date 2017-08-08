package edu.sdsc.mmtf.spark.mappers;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.vecmath.Matrix4f;
import javax.vecmath.Point3f;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.rcsb.mmtf.api.StructureDataInterface;
import org.rcsb.mmtf.decoder.DecoderUtils;
import org.rcsb.mmtf.encoder.AdapterToStructureData;
import org.spark_project.guava.primitives.Doubles;
import org.spark_project.guava.primitives.Floats;

import scala.Tuple2;
/**
 * TODO 
 * @author Yue Yu
 */
public class StructureToBioassembly implements PairFlatMapFunction<Tuple2<String,StructureDataInterface>,String, StructureDataInterface> {
	private static final long serialVersionUID = -9098891849038187334L;

	@Override
	public Iterator<Tuple2<String, StructureDataInterface>> call(Tuple2<String, StructureDataInterface> t) throws Exception {
		StructureDataInterface structure = t._2;
		//Map<Integer, Integer> atomMap = new HashMap<>();
		List<Tuple2<String, StructureDataInterface>> resList = new ArrayList<>();
		//get the number of bioassemblies that the structure has.
		//for each of them, create one structure.
		for (int i = 0; i < structure.getNumBioassemblies(); i++) {
			//initiate the bioassembly structure.
			AdapterToStructureData bioAssembly = new AdapterToStructureData();
			//set the structureID.
			String structureId = structure.getStructureId() + "-BioAssembly" + structure.getBioassemblyName(i);
			
			int totAtoms = 0, totBonds = 0, totGroups = 0, totChains = 0, totModels = 0;
			int numTrans = structure.getNumTransInBioassembly(i);
			totModels = structure.getNumModels();
			
			int[][] bioChainList = new int[numTrans][];
			double[][] transMatrix = new double[numTrans][];
			//loop through all the trans current bioassembly structure has.
			//calculate the total data we will use to initialize the structure.
			for(int ii = 0; ii < numTrans; ii++)
			{
				bioChainList[ii] = structure.getChainIndexListForTransform(i, ii);
				transMatrix[ii] = structure.getMatrixForTransform(i, ii);
				for (int j = 0; j < totModels; j++)
				{
					totChains += bioChainList[ii].length;
					//System.out.println(bioChainList[ii].length);
					for (int k = 0, groupCounter = 0; k < structure.getChainsPerModel()[j]; k++)
					{
						boolean adding = false;
						for(int currChain : bioChainList[ii])
						{
							if(currChain == k) adding = true;
						}
						if(adding)
						{
							//System.out.println("adding groups");
							totGroups += structure.getGroupsPerChain()[k];
						}
						for (int h = 0; h < structure.getGroupsPerChain()[k]; h++, groupCounter++){
							if(adding)
							{
								int groupIndex = structure.getGroupTypeIndices()[groupCounter];	
								totAtoms += structure.getNumAtomsInGroup(groupIndex);
								totBonds += structure.getGroupBondOrders(groupIndex).length;
							}
						}
					}
				}
			}
			//init
			System.out.println("Initializing the structure with\n"
					+ " totModel = " + totModels + ", totChains = " + totChains + ", totGroups = " + totGroups + ", totAtoms = " 
					+ totAtoms + ", totBonds = " + totBonds + ", name : " + structureId);
			bioAssembly.initStructure(totBonds, totAtoms, totGroups, totChains, totModels, structureId);
			DecoderUtils.addXtalographicInfo(structure, bioAssembly);
			DecoderUtils.addHeaderInfo(structure, bioAssembly);	
			
			/*
			 * Now we have bioChainList and transMatrix.
			 * bioChainList[i] is the ith trans' list of chains it has.  
			 * transMatrix[i] is the matrix that is going to be applied on those chains.
			 */	
			//initialize the indices.
			int modelIndex = 0;
			int chainIndex = 0;
			int groupIndex = 0;
			int atomIndex = 0;
			int chainCounter = 0;
//			int gbIndex = 0;
			// loop through models
			for(int ii = 0; ii < structure.getNumModels(); ii++)
			{
				
				// precalculate indices
				int numChainsPerModel = structure.getChainsPerModel()[modelIndex] * numTrans;
				bioAssembly.setModelInfo(modelIndex, numChainsPerModel);
				int[] chainToEntityIndex = getChainToEntityIndex(structure);
				//loop through chains
				for(int j = 0; j < structure.getChainsPerModel()[modelIndex]; j++)
				{
					
					//loop through each trans
					int currGroupIndex = groupIndex;
					int currAtomIndex = atomIndex;
					
					for (int k = 0; k < numTrans; k++)
					{
						
						//get the currChainList that needs to be added
						int[] currChainList = bioChainList[k];
						double[] currMatrix = transMatrix[k];
						boolean addThisChain = false;
						for(int currChain : currChainList)
						{
							if(currChain == j) addThisChain = true;
						}

						groupIndex = currGroupIndex;
						atomIndex = currAtomIndex;
						float[] xCoords = structure.getxCoords();
						float[] yCoords = structure.getyCoords();
						float[] zCoords = structure.getzCoords();
						float[] floatMatrix = Floats.toArray(Doubles.asList(currMatrix));
						Matrix4f m = new Matrix4f(floatMatrix);
						
						if(addThisChain){	
							int entityToChainIndex = chainToEntityIndex[chainIndex];
							System.out.println("adding chain : " + chainIndex);
							//TODO
							//not sure
							bioAssembly.setEntityInfo(new int[]{chainCounter}, structure.getEntitySequence(entityToChainIndex), 
									structure.getEntityDescription(entityToChainIndex), structure.getEntityType(entityToChainIndex));
							bioAssembly.setChainInfo(structure.getChainIds()[chainIndex], structure.getChainNames()[chainIndex],
									structure.getGroupsPerChain()[chainIndex]);
							chainCounter ++;			
						}
						//loop through the groups in the chain
						for(int jj = 0; jj < structure.getGroupsPerChain()[chainIndex]; jj++)
						{
							int currgroup = structure.getGroupTypeIndices()[groupIndex];
							
							if(addThisChain)
							{					
								bioAssembly.setGroupInfo(structure.getGroupName(currgroup), structure.getGroupIds()[groupIndex], 
										structure.getInsCodes()[groupIndex], structure.getGroupChemCompType(currgroup),
										structure.getNumAtomsInGroup(currgroup),structure.getGroupBondOrders(currgroup).length, 
										structure.getGroupSingleLetterCode(currgroup), structure.getGroupSequenceIndices()[groupIndex], 
										structure.getSecStructList()[groupIndex]);								
							}
							for(int kk = 0; kk < structure.getNumAtomsInGroup(currgroup); kk++)
							{
								//System.out.println("currgroup : " + currgroup + " curratom : " + kk);
								if(addThisChain)
								{
									Point3f p1 = new Point3f(xCoords[atomIndex], yCoords[atomIndex], zCoords[atomIndex]);
									m.transform(p1);
									//System.out.println(kk + " " + currgroup);
									bioAssembly.setAtomInfo(structure.getGroupAtomNames(currgroup)[kk], structure.getAtomIds()[atomIndex], 
											structure.getAltLocIds()[atomIndex],p1.x, p1.y, p1.z, 
											structure.getOccupancies()[atomIndex], structure.getbFactors()[atomIndex],
											structure.getGroupElementNames(currgroup)[kk], structure.getGroupAtomCharges(currgroup)[kk]);
								}
								//inc the atomIndex
								atomIndex++;
							}							
							if(addThisChain)
							{
								for (int l = 0; l < structure.getGroupBondOrders(currgroup).length; l++) {
								//	System.out.println(structure.getGroupBondOrders(currgroup).length + " " + l);
									int bondIndOne = structure.getGroupBondIndices(currgroup)[l*2];
									int bondIndTwo = structure.getGroupBondIndices(currgroup)[l*2+1];
									int bondOrder = structure.getGroupBondOrders(currgroup)[l];
									bioAssembly.setGroupBond(bondIndOne, bondIndTwo, bondOrder);
								}
							}
							//inc the groupIndex
							groupIndex++;
						}
						if (addThisChain) {
							// Add inter-group bond info
//							for(int l = 0;  l < structure.getInterGroupBondOrders().length; l++){
//								int bondIndOne = structure.getInterGroupBondIndices()[l*2];
//								int bondIndTwo = structure.getInterGroupBondIndices()[l*2+1];
//								int bondOrder = structure.getInterGroupBondOrders()[l];
//								Integer indexOne = atomMap.get(bondIndOne);
//								if (indexOne != null) {
//									Integer indexTwo = atomMap.get(bondIndTwo);
//									if (indexTwo != null) {
//										bioAssembly.setInterGroupBond(indexOne, indexTwo, bondOrder);
//									}
//								}
							}
					}
					//inc the chainIndex
					chainIndex++;
				}
				//inc the modelIndex
				modelIndex++;
			}
			
			
			bioAssembly.finalizeStructure();
			resList.add(new Tuple2<String, StructureDataInterface>(structureId, bioAssembly));
		}
		return resList.iterator();
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