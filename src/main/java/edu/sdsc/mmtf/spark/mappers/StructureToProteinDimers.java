package edu.sdsc.mmtf.spark.mappers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.vecmath.Point3d;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.biojava.nbio.structure.symmetry.geometry.DistanceBox;
import org.rcsb.mmtf.api.StructureDataInterface;
import org.rcsb.mmtf.decoder.DecoderUtils;
import org.rcsb.mmtf.encoder.AdapterToStructureData;
import org.rcsb.mmtf.encoder.EncoderUtils;

import edu.sdsc.mmtf.spark.filters.ContainsPolymerChainType;
import scala.Tuple2;
/**
 * TODO 
 * @author Yue Yu
 */
public class StructureToProteinDimers implements PairFlatMapFunction<Tuple2<String,StructureDataInterface>,String, StructureDataInterface> {


	private static final long serialVersionUID = 590629701792189982L;
	private double cutoffDistance = 8.0;
	private int contacts = 20;
	private boolean useAllAtoms = false;
	private boolean exclusive = false;
	/**
	 * 
	 * 
	 */
	public StructureToProteinDimers() {}
	
	/**
	 * 
	 * 
	 */
	public StructureToProteinDimers(double cutoffDistance, int contacts) {
		this.cutoffDistance = cutoffDistance;
		this.contacts = contacts;
	}
	
	/**
	 * 
	 * 
	 */
	public StructureToProteinDimers(double cutoffDistance, int contacts, boolean useAllAtoms, boolean exclusive) {
		this.cutoffDistance = cutoffDistance;
		this.contacts = contacts;
		this.useAllAtoms = useAllAtoms;
		this.exclusive = exclusive;
	}
		
	@Override
	public Iterator<Tuple2<String, StructureDataInterface>> call(Tuple2<String, StructureDataInterface> t) throws Exception {
		StructureDataInterface structure = t._2;
		List<Tuple2<String, StructureDataInterface>> resList = new ArrayList<>();
		
		//split the structure into a list of structure of chains
		List<StructureDataInterface> chains = splitToChains(structure);
		//for each chain, create a distance box
		List<DistanceBox<Integer>> boxes;
		if(useAllAtoms == true)
			boxes = getAllAtomsDistanceBoxes(chains, cutoffDistance);
		else boxes = getCBetaAtomsDistanceBoxes(chains, cutoffDistance);
		
		HashSet<Tuple2<String, String>> exclusiveHashSet = new HashSet<Tuple2<String, String>>();
		//loop through chains
		for(int i = 0; i < chains.size(); i++)
		{
			for(int j = 0; j < i; j++)
			{	
				
				//for each pair of chains, check if they are in contact or not
				if(checkPair(boxes.get(i), boxes.get(j), chains.get(i), chains.get(j), cutoffDistance, contacts))
				{
					if(exclusive)
					{
						String es1 = chains.get(i).getEntitySequence(getChainToEntityIndex(chains.get(i))[0]);
						String es2 = chains.get(j).getEntitySequence(getChainToEntityIndex(chains.get(j))[0]);
						Tuple2<String, String> newTuple1 = new Tuple2<String, String>(es1, es2);
						Tuple2<String, String> newTuple2 = new Tuple2<String, String>(es2, es1);
						if(!exclusiveHashSet.contains(newTuple1) && !exclusiveHashSet.contains(newTuple2))
						{
							resList.add(combineChains(chains.get(i), chains.get(j)));
							exclusiveHashSet.add(newTuple1);
							exclusiveHashSet.add(newTuple2);
						}
					}
					else resList.add(combineChains(chains.get(i), chains.get(j)));
				}
			}
		}
		return resList.iterator();
	}

	private static double distance(StructureDataInterface s1, StructureDataInterface s2, Integer index1, Integer index2 )
	{
		double xCoord = s1.getxCoords()[index1];
		double yCoord = s1.getyCoords()[index1];
		double zCoord = s1.getzCoords()[index1];
		Point3d newPoint1 = new Point3d(xCoord, yCoord, zCoord);
		xCoord = s2.getxCoords()[index2];
		yCoord = s2.getyCoords()[index2];
		zCoord = s2.getzCoords()[index2];
		Point3d newPoint2 = new Point3d(xCoord, yCoord, zCoord);
		return newPoint1.distance(newPoint2);
	}
	
	private static boolean checkPair(DistanceBox<Integer> box1, DistanceBox<Integer> box2,
			StructureDataInterface s1, StructureDataInterface s2, double cutoffDistance, int contacts)
	{
		List<Integer> pointsInBox2= box1.getIntersection(box2);		
		List<Integer> pointsInBox1= box2.getIntersection(box1);	
		HashSet<Integer> hs1 = new HashSet<Integer>();
		HashSet<Integer> hs2 = new HashSet<Integer>();		
		int num = 0;
		for(int i = 0; i < pointsInBox2.size(); i++)
		{
			for(int j = 0; j < pointsInBox1.size(); j++)
			{
				if(hs1.contains(i) || hs2.contains(j)) continue;
				if(distance(s1, s2, pointsInBox2.get(i), pointsInBox1.get(j)) < cutoffDistance)
				{
					num++;
					hs1.add(i);
					hs2.add(j);
				}
				if(num > contacts)	return true;
			}
		}
		return false;
	}
	
	private static List<DistanceBox<Integer>> getAllAtomsDistanceBoxes(List<StructureDataInterface> chains, double cutoffDistance)
	{
		List<DistanceBox<Integer>> distanceBoxes = new ArrayList<DistanceBox<Integer>>();
		for(int i = 0; i < chains.size(); i++)
		{
			StructureDataInterface tmp = chains.get(i);
			DistanceBox<Integer> newBox = new DistanceBox<Integer>(cutoffDistance);
			//System.out.println(tmp.getNumAtoms());
			for(int j = 0; j <tmp.getNumAtoms(); j++)
			{
				double xCoord = tmp.getxCoords()[j];
				double yCoord = tmp.getyCoords()[j];
				double zCoord = tmp.getzCoords()[j];
				Point3d newPoint = new Point3d(xCoord, yCoord, zCoord);
				//System.out.println(newPoint);
				newBox.addPoint(newPoint, j);
			}
			distanceBoxes.add(newBox);
		}
		return distanceBoxes;
	}
	
	private static List<DistanceBox<Integer>> getCBetaAtomsDistanceBoxes(List<StructureDataInterface> chains, double cutoffDistance)
	{
		List<DistanceBox<Integer>> distanceBoxes = new ArrayList<DistanceBox<Integer>>();
		for(int i = 0; i < chains.size(); i++)
		{
			StructureDataInterface tmp = chains.get(i);
			DistanceBox<Integer> newBox = new DistanceBox<Integer>(cutoffDistance);
			int groupIndex = 0;
			int atomIndex = 0;
			for (int k = 0; k < tmp.getGroupsPerChain()[0]; k++) {		
				int groupType = tmp.getGroupTypeIndices()[groupIndex];	
				for (int m = 0; m < tmp.getNumAtomsInGroup(groupType); m++)
				{
					String atomName = tmp.getGroupAtomNames(groupType)[m];
					if(atomName.equals("CB"))
					{
						double xCoord = tmp.getxCoords()[atomIndex];
						double yCoord = tmp.getyCoords()[atomIndex];
						double zCoord =tmp.getzCoords()[atomIndex]; 
						Point3d newPoint = new Point3d(xCoord, yCoord, zCoord);
						newBox.addPoint(newPoint, atomIndex);
					}
					atomIndex++;
				}
				groupIndex++;
			}
			distanceBoxes.add(newBox);
		}
		return distanceBoxes;
	}
	
	private static List<StructureDataInterface> splitToChains(StructureDataInterface s)
	{
		List<StructureDataInterface> chains = new ArrayList<StructureDataInterface>();
		int numChains = s.getChainsPerModel()[0];
		int[] chainToEntityIndex = getChainToEntityIndex(s);
		int[] atomsPerChain = new int[numChains];
		int[] bondsPerChain = new int[numChains];
		getNumAtomsAndBonds(s, atomsPerChain, bondsPerChain);
		for (int i = 0, atomCounter = 0, groupCounter = 0; i < numChains; i++){	
			AdapterToStructureData newChain = new AdapterToStructureData();
			int entityToChainIndex = chainToEntityIndex[i];
			Map<Integer, Integer> atomMap = new HashMap<>();

	        // to avoid of information loss, add chainName/IDs and entity id
			// this required by some queries
			String structureId = s.getStructureId() + "." + s.getChainNames()[i] +
					"." + s.getChainIds()[i] + "." + (entityToChainIndex+1);
			
			// set header
			newChain.initStructure(bondsPerChain[i], atomsPerChain[i], 
					s.getGroupsPerChain()[i], 1, 1, structureId);
			DecoderUtils.addXtalographicInfo(s, newChain);
			DecoderUtils.addHeaderInfo(s, newChain);	

			// set model info (only one model: 0)
			newChain.setModelInfo(0, 1);
			// set entity and chain info
			newChain.setEntityInfo(new int[]{0}, s.getEntitySequence(entityToChainIndex), 
					s.getEntityDescription(entityToChainIndex), s.getEntityType(entityToChainIndex));
			newChain.setChainInfo(s.getChainIds()[i], s.getChainNames()[i], s.getGroupsPerChain()[i]);

			for (int j = 0; j < s.getGroupsPerChain()[i]; j++, groupCounter++){
				int groupIndex = s.getGroupTypeIndices()[groupCounter];
				// set group info
				newChain.setGroupInfo(s.getGroupName(groupIndex), s.getGroupIds()[groupCounter], 
						s.getInsCodes()[groupCounter], s.getGroupChemCompType(groupIndex),
						s.getNumAtomsInGroup(groupIndex), s.getGroupBondOrders(groupIndex).length,
						s.getGroupSingleLetterCode(groupIndex), s.getGroupSequenceIndices()[groupCounter], 
						s.getSecStructList()[groupCounter]);

				for (int k = 0; k < s.getNumAtomsInGroup(groupIndex); k++, atomCounter++){		
					newChain.setAtomInfo(s.getGroupAtomNames(groupIndex)[k], s.getAtomIds()[atomCounter], 
							s.getAltLocIds()[atomCounter], s.getxCoords()[atomCounter], s.getyCoords()[atomCounter],
							s.getzCoords()[atomCounter], s.getOccupancies()[atomCounter], s.getbFactors()[atomCounter],
							s.getGroupElementNames(groupIndex)[k], s.getGroupAtomCharges(groupIndex)[k]);

				}

				// add intra-group bond info
				for (int l = 0; l < s.getGroupBondOrders(groupIndex).length; l++) {
					int bondIndOne = s.getGroupBondIndices(groupIndex)[l*2];
					int bondIndTwo = s.getGroupBondIndices(groupIndex)[l*2+1];
					int bondOrder = s.getGroupBondOrders(groupIndex)[l];
					newChain.setGroupBond(bondIndOne, bondIndTwo, bondOrder);
					
				}
			}

			// Add inter-group bond info
			for(int ii = 0; ii < s.getInterGroupBondOrders().length; ii++){
				int bondIndOne = s.getInterGroupBondIndices()[ii*2];
				int bondIndTwo = s.getInterGroupBondIndices()[ii*2+1];
				int bondOrder = s.getInterGroupBondOrders()[ii];
				Integer indexOne = atomMap.get(bondIndOne);
				if (indexOne != null) {
					Integer indexTwo = atomMap.get(bondIndTwo);
					if (indexTwo != null) {
						newChain.setInterGroupBond(indexOne, indexTwo, bondOrder);
					}
				}
			}
			newChain.finalizeStructure();
			
			if(EncoderUtils.getTypeFromChainId(newChain, 0).equals("polymer"))
			{
				boolean match = true;
				for (int j = 0; j < newChain.getGroupsPerChain()[0]; j++) {			
					if (match) {
						int groupIndex = newChain.getGroupTypeIndices()[j];
						String type = newChain.getGroupChemCompType(groupIndex);
						//System.out.println(j + " " + type);
						match = type.equals(ContainsPolymerChainType.L_PEPTIDE_LINKING) || 
								type.equals(ContainsPolymerChainType.PEPTIDE_LINKING);
					}
				}
				if(match) chains.add(newChain);
			}
		}
		return chains;
	}
	
	/**
	 * A method that takes two structure of chains and return a single structur of two chains.
	 */
	private static Tuple2<String, StructureDataInterface> combineChains(StructureDataInterface s1, StructureDataInterface s2)
	{
		int groupCounter = 0;
		int atomCounter = 0;
		
		String structureId = s1.getStructureId() + "_append_" + s2.getStructureId();
		AdapterToStructureData combinedStructure = new AdapterToStructureData(); 
		combinedStructure.initStructure(s1.getNumBonds() + s2.getNumBonds(), s1.getNumAtoms() + s2.getNumAtoms(),
				s1.getNumGroups() + s2.getNumGroups(), 2, 1, structureId);
		DecoderUtils.addXtalographicInfo(s1, combinedStructure);
		DecoderUtils.addHeaderInfo(s1, combinedStructure);	
		
		combinedStructure.setModelInfo(0, 2);

		// set entity and chain info
		combinedStructure.setEntityInfo(new int[]{0}, s1.getEntitySequence(getChainToEntityIndex(s1)[0]), 
				s1.getEntityDescription(getChainToEntityIndex(s1)[0]), s1.getEntityType(getChainToEntityIndex(s1)[0]));
		combinedStructure.setChainInfo(s1.getChainIds()[0], s1.getChainNames()[0], s1.getGroupsPerChain()[0]);
		
		for (int i = 0; i < s1.getGroupsPerChain()[0]; i++, groupCounter++){
			int groupIndex = s1.getGroupTypeIndices()[groupCounter];
			// set group info
			combinedStructure.setGroupInfo(s1.getGroupName(groupIndex), s1.getGroupIds()[groupCounter], 
					s1.getInsCodes()[groupCounter], s1.getGroupChemCompType(groupIndex), s1.getNumAtomsInGroup(groupIndex),
					s1.getGroupBondOrders(groupIndex).length, s1.getGroupSingleLetterCode(groupIndex),
					s1.getGroupSequenceIndices()[groupCounter], s1.getSecStructList()[groupCounter]);

			for (int j = 0; j < s1.getNumAtomsInGroup(groupIndex); j++, atomCounter++){
				combinedStructure.setAtomInfo(s1.getGroupAtomNames(groupIndex)[j], s1.getAtomIds()[atomCounter],
						s1.getAltLocIds()[atomCounter], s1.getxCoords()[atomCounter], s1.getyCoords()[atomCounter], 
						s1.getzCoords()[atomCounter], s1.getOccupancies()[atomCounter], s1.getbFactors()[atomCounter],
						s1.getGroupElementNames(groupIndex)[j], s1.getGroupAtomCharges(groupIndex)[j]);
			}
			//TODO : not sure if we should add bonds like this.
			for (int j = 0; j < s1.getGroupBondOrders(groupIndex).length; j++) {
				int bondIndOne = s1.getGroupBondIndices(groupIndex)[j*2];
				int bondIndTwo = s1.getGroupBondIndices(groupIndex)[j*2+1];
				int bondOrder = s1.getGroupBondOrders(groupIndex)[j];
				combinedStructure.setGroupBond(bondIndOne, bondIndTwo, bondOrder);
			}
		}
		// set entity and chain info
		combinedStructure.setEntityInfo(new int[]{1}, s1.getEntitySequence(getChainToEntityIndex(s2)[0]), 
				s2.getEntityDescription(getChainToEntityIndex(s2)[0]), s2.getEntityType(getChainToEntityIndex(s2)[0]));
		combinedStructure.setChainInfo(s2.getChainIds()[0], s2.getChainNames()[0], s2.getGroupsPerChain()[0]);
		groupCounter = 0;
		atomCounter = 0;
		for (int i = 0; i < s2.getGroupsPerChain()[0]; i++, groupCounter++){
			int groupIndex = s2.getGroupTypeIndices()[groupCounter];
			// set group info
			combinedStructure.setGroupInfo(s2.getGroupName(groupIndex), s2.getGroupIds()[groupCounter], 
					s2.getInsCodes()[groupCounter], s2.getGroupChemCompType(groupIndex), s2.getNumAtomsInGroup(groupIndex),
					s2.getGroupBondOrders(groupIndex).length, s2.getGroupSingleLetterCode(groupIndex),
					s2.getGroupSequenceIndices()[groupCounter], s2.getSecStructList()[groupCounter]);

			for (int j = 0; j < s2.getNumAtomsInGroup(groupIndex); j++, atomCounter++){
				combinedStructure.setAtomInfo(s2.getGroupAtomNames(groupIndex)[j], s2.getAtomIds()[atomCounter],
						s2.getAltLocIds()[atomCounter], s2.getxCoords()[atomCounter], s2.getyCoords()[atomCounter], 
						s2.getzCoords()[atomCounter], s2.getOccupancies()[atomCounter], s2.getbFactors()[atomCounter],
						s2.getGroupElementNames(groupIndex)[j], s2.getGroupAtomCharges(groupIndex)[j]);
			}
			//TODO : not sure if we should add bonds like this.
			for (int j = 0; j < s2.getGroupBondOrders(groupIndex).length; j++) {
				int bondIndOne = s2.getGroupBondIndices(groupIndex)[j*2];
				int bondIndTwo = s2.getGroupBondIndices(groupIndex)[j*2+1];
				int bondOrder = s2.getGroupBondOrders(groupIndex)[j];
				combinedStructure.setGroupBond(bondIndOne, bondIndTwo, bondOrder);
			}
		}
		combinedStructure.finalizeStructure();
		return (new Tuple2<String, StructureDataInterface>(structureId, combinedStructure));
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