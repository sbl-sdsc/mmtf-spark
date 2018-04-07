package edu.sdsc.mmtf.spark.mappers;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.rcsb.mmtf.api.StructureDataInterface;
import org.rcsb.mmtf.decoder.DecoderUtils;
import org.rcsb.mmtf.encoder.AdapterToStructureData;

import scala.Tuple2;

/**
 * COMMENT TODO
 * @author Yue Yu
 */
public class StructureToCathDomains implements PairFlatMapFunction<Tuple2<String,StructureDataInterface>,String, StructureDataInterface> {

	private static final long serialVersionUID = -474199780109818259L;
	private HashMap<String, ArrayList<String>> hmap;
	
	public static HashMap<String, ArrayList<String>> getMap(String url) throws IOException
	{
		
		HashMap<String, ArrayList<String>> hmap = new HashMap<String, ArrayList<String>>();
	    
		URL u = new URL(url);
	    InputStream in = u.openStream();
		BufferedReader rd = new BufferedReader(new InputStreamReader(new GZIPInputStream(in)));

		String key, value, line;

		while ((line = rd.readLine()) != null) {

//			System.out.println(line);
			key = line.substring(0,5).toUpperCase();
			value = line.substring(8).split(" ")[2];
//			System.out.println(key+"\n"+value);
			if(!hmap.containsKey(key))
			{
				ArrayList<String> tmp = new ArrayList<String>();
				tmp.add(value);
				hmap.put(key, tmp);
			}
			else{
				ArrayList<String> tmp = hmap.get(key);
				tmp.add(value);
				hmap.replace(key, tmp);
				
			}
	    
		}
		return hmap;
	}
	

	/**
	 * COMMENT TODO
	 */
	public StructureToCathDomains() {}
	
	/**
	 * COMMENT TODO
	 */
	public StructureToCathDomains(HashMap<String, ArrayList<String>> hmap) {
		this.hmap = hmap;
	}
	
	@Override
	public Iterator<Tuple2<String, StructureDataInterface>> call(Tuple2<String, StructureDataInterface> t) throws Exception {
		StructureDataInterface structure = t._2;

		// precalculate indices
		int numChains = structure.getChainsPerModel()[0];
		int[] chainToEntityIndex = getChainToEntityIndex(structure);
		int[] atomsPerChain = new int[numChains];
		int[] bondsPerChain = new int[numChains];
		getNumAtomsAndBonds(structure, atomsPerChain, bondsPerChain);
		
		List<Tuple2<String, StructureDataInterface>> chainList = new ArrayList<>();

//		System.out.println(numChains);
		for (int i = 0, atomCounter = 0, groupCounter = 0; i < numChains; i++){	
			AdapterToStructureData cathDomain = new AdapterToStructureData();
			
			
			String key = structure.getStructureId() + structure.getChainNames()[i];
			boolean newChain = hmap.containsKey(key);

			if(newChain)
			{
//				System.out.println(key);
				ArrayList<String> values = hmap.get(key);
				for(int valueNum = 0; valueNum < values.size(); valueNum++)
				{
					
					String groupInfo = values.get(valueNum);
					
					int[][] groupBounds = clacGroupBound(groupInfo);
					
					int tmpGroupCounter = groupCounter;
					int tmpAtomCounter = atomCounter;
					int entityToChainIndex = chainToEntityIndex[i];
					int cathAtomCount = 0;
					
					Map<Integer, Integer> atomMap = new HashMap<>();
		
			        // to avoid of information loss, add chainName/IDs and entity id
					// this required by some queries
					String structureId = structure.getStructureId() + "." + structure.getChainNames()[i] +
							"." + structure.getChainIds()[i] + "." + (entityToChainIndex+1);
					
					// set header
					cathDomain.initStructure(bondsPerChain[i], atomsPerChain[i], 
							structure.getGroupsPerChain()[i], 1, 1, structureId);
					DecoderUtils.addXtalographicInfo(structure, cathDomain);
					DecoderUtils.addHeaderInfo(structure, cathDomain);	
	
					// set model info (only one model: 0)
					cathDomain.setModelInfo(0, 1);
	
					// set entity and chain info
					cathDomain.setEntityInfo(new int[]{0}, structure.getEntitySequence(entityToChainIndex), 
							structure.getEntityDescription(entityToChainIndex), structure.getEntityType(entityToChainIndex));
					cathDomain.setChainInfo(structure.getChainIds()[i], structure.getChainNames()[i], structure.getGroupsPerChain()[i]);
		
					for (int j = 0; j < structure.getGroupsPerChain()[i]; j++, tmpGroupCounter++){
						int groupIndex = structure.getGroupTypeIndices()[tmpGroupCounter];
						
						boolean inBound = false;
						
						int groupId = structure.getGroupIds()[tmpGroupCounter];
						for(int boundIndex = 0; boundIndex < groupBounds.length; boundIndex++)
						{
//							System.out.println(j);
							if(groupId >= groupBounds[boundIndex][0] && groupId <= groupBounds[boundIndex][1])
							{
//								System.out.println(groupBounds[boundIndex][0] + " " + (j+1) + " " +groupBounds[boundIndex][1]);
								inBound = true;
							}
						}
						
						if (inBound) {
							// set group info
							cathDomain.setGroupInfo(structure.getGroupName(groupIndex), structure.getGroupIds()[tmpGroupCounter], 
									structure.getInsCodes()[tmpGroupCounter], structure.getGroupChemCompType(groupIndex), structure.getNumAtomsInGroup(groupIndex),
									structure.getGroupBondOrders(groupIndex).length, structure.getGroupSingleLetterCode(groupIndex), structure.getGroupSequenceIndices()[tmpGroupCounter], 
									structure.getSecStructList()[tmpGroupCounter]);
						}
		
						for (int k = 0; k < structure.getNumAtomsInGroup(groupIndex); k++, tmpAtomCounter++){
							if (inBound) {
								// set atom info
								atomMap.put(tmpAtomCounter, cathAtomCount);
								cathAtomCount++;
								
								cathDomain.setAtomInfo(structure.getGroupAtomNames(groupIndex)[k], structure.getAtomIds()[tmpAtomCounter], structure.getAltLocIds()[tmpAtomCounter], 
										structure.getxCoords()[tmpAtomCounter], structure.getyCoords()[tmpAtomCounter], structure.getzCoords()[tmpAtomCounter], 
										structure.getOccupancies()[tmpAtomCounter], structure.getbFactors()[tmpAtomCounter], structure.getGroupElementNames(groupIndex)[k], structure.getGroupAtomCharges(groupIndex)[k]);
							}
						}
		
						if (inBound) {
							// add intra-group bond info
							for (int l = 0; l < structure.getGroupBondOrders(groupIndex).length; l++) {
								int bondIndOne = structure.getGroupBondIndices(groupIndex)[l*2];
								int bondIndTwo = structure.getGroupBondIndices(groupIndex)[l*2+1];
								int bondOrder = structure.getGroupBondOrders(groupIndex)[l];
								cathDomain.setGroupBond(bondIndOne, bondIndTwo, bondOrder);
							}
						}
					}
		
					
					// Add inter-group bond info
					for(int ii = 0; ii < structure.getInterGroupBondOrders().length; ii++){
						int bondIndOne = structure.getInterGroupBondIndices()[ii*2];
						int bondIndTwo = structure.getInterGroupBondIndices()[ii*2+1];
						int bondOrder = structure.getInterGroupBondOrders()[ii];
						Integer indexOne = atomMap.get(bondIndOne);
						if (indexOne != null) {
							Integer indexTwo = atomMap.get(bondIndTwo);
							if (indexTwo != null) {
								cathDomain.setInterGroupBond(indexOne, indexTwo, bondOrder);
							}
						}
					}
	
					cathDomain.finalizeStructure();
				
					String chId = structure.getChainNames()[i];
					String cathId = Integer.toString(valueNum);
					chainList.add(new Tuple2<String, StructureDataInterface>(structure.getStructureId() + "." + chId + cathId, cathDomain));
				
					if(valueNum == values.size()-1)
					{
						groupCounter = tmpGroupCounter;
						atomCounter = tmpAtomCounter;
					}
				
				}
				hmap.remove(key);
			}
			else{
				for (int j = 0; j < structure.getGroupsPerChain()[i]; j++, groupCounter++){
					int groupIndex = structure.getGroupTypeIndices()[groupCounter];
					for (int k = 0; k < structure.getNumAtomsInGroup(groupIndex); k++, atomCounter++){}
				}
			}
		}

		return chainList.iterator();
	}


	private int[][] clacGroupBound(String groupInfo) {

		String[] tmp = groupInfo.split(" ");		
		int[][] groupBound = new int [tmp.length][2];
		for(int i = 0; i < tmp.length; i++)
		{
//			System.out.println(tmp[i]);
			String[] bounds = tmp[i].split(":")[0].split("-");
			groupBound[i][0] = Integer.parseInt(bounds[0]);
			groupBound[i][1] = Integer.parseInt(bounds[1]);
		}
		return groupBound;
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