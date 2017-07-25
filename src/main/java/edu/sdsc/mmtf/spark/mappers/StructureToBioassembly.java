package edu.sdsc.mmtf.spark.mappers;

import java.util.ArrayList;
import java.util.Arrays;
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
			//init
			bioAssembly.initStructure(totBonds * numTrans, totAtoms * numTrans,
					totGroups * numTrans, totChains * numTrans, totModels * numTrans, structureId);
			DecoderUtils.addXtalographicInfo(structure, bioAssembly);
			DecoderUtils.addHeaderInfo(structure, bioAssembly);	
			
			System.out.println("bioassembly: " + structure.getBioassemblyName(i));
			int numTransformations = structure.getNumTransInBioassembly(i);
			System.out.println("  Number transformations: " + numTransformations);
			
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
//			int groupCounter = 0;
//			int atomCounter = 0;
			
			
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
						boolean addThisChain = Arrays.asList(currChainList).contains(chainIndex);

						groupIndex = currGroupIndex;
						atomIndex = currAtomIndex;
						float[] xCoords = structure.getxCoords();
						float[] yCoords = structure.getyCoords();
						float[] zCoords = structure.getzCoords();
						float[] floatMatrix = Floats.toArray(Doubles.asList(currMatrix));
						Matrix4f m = new Matrix4f(floatMatrix);
						
						if(addThisChain){	
							int entityToChainIndex = chainToEntityIndex[chainIndex];
							
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
							if(addThisChain)
							{
								int currgroup = structure.getGroupTypeIndices()[groupIndex];
								bioAssembly.setGroupInfo(structure.getGroupName(currgroup), structure.getGroupIds()[groupIndex], 
										structure.getInsCodes()[groupIndex], structure.getGroupChemCompType(currgroup),
										structure.getNumAtomsInGroup(currgroup),structure.getGroupBondOrders(currgroup).length, 
										structure.getGroupSingleLetterCode(currgroup), structure.getGroupSequenceIndices()[groupIndex], 
										structure.getSecStructList()[groupIndex]);
								
//								groupCounter++;
							}
							for(int kk = 0; kk < structure.getNumAtomsInGroup(groupIndex); kk++)
							{
								if(addThisChain)
								{
									Point3f p1 = new Point3f(xCoords[atomIndex], yCoords[atomIndex], zCoords[atomIndex]);
									m.transform(p1);
									bioAssembly.setAtomInfo(structure.getGroupAtomNames(groupIndex)[kk], structure.getAtomIds()[atomIndex], 
											structure.getAltLocIds()[atomIndex],p1.x, p1.y, p1.z, 
											structure.getOccupancies()[atomIndex], structure.getbFactors()[atomIndex],
											structure.getGroupElementNames(groupIndex)[kk], structure.getGroupAtomCharges(groupIndex)[kk]);
//									atomCounter++;
								}
								//inc the atomIndex
								atomIndex++;
							}
							//inc the groupIndex
							groupIndex++;
						}
					
					}
					
					
					//inc the chainIndex
					chainIndex++;
				}
				//inc the modelIndex
				modelIndex++;
//				for (int j = 0; j < numTransformations; j++) {
//					System.out.println("    transformation: " + j);
//					System.out.println("    chains:         " + Arrays.toString(structure.getChainIndexListForTransform(i, j)));
//					System.out.println("    rotTransMatrix: " + Arrays.toString(structure.getMatrixForTransform(i, j)));
//				}
			}	
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