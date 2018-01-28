package edu.sdsc.mmtf.spark.webfilters;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.spark.api.java.function.Function;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.webservices.AdvancedQueryService;
import scala.Tuple2;

/**
 * This filter runs an RCSB PDB Advanced Search web service using an XML query description.
 * 
 * <p>See <a href="https://www.rcsb.org/pdb/staticHelp.do?p=help/advancedSearch.html"> Advanced Search</a>
 * 
 * <p>Example: find PDB entries that contain the word "mutant" in the structure title:
 * 
 * <pre><code>
 *      JavaPairRDD<String, StructureDataInterface> pdb = ...
 *      String query = "<orgPdbQuery>
 *                          "<queryType>org.pdb.query.simple.StructTitleQuery</queryType>" +
 *                          "<struct.title.comparator>contains</struct.title.comparator>" +
 *                          "<struct.title.value>mutant</struct.title.value" +
 *                     "</orgPdbQuery>";
 *      pdb = pdb.filter(new RcsbAdvancedSearch(query));
 * </code></pre>
 * 
 * @author Peter Rose
 *
 */
public class AdvancedQuery implements Function<Tuple2<String, StructureDataInterface>, Boolean> {
	private static final long serialVersionUID = -4794067375376198086L;
	private Set<String> structureIds;
	private boolean entityLevel = false;
	private boolean exclusive = false;

	/**
	 * Filters using the RCSB PDB Advanced Search web service
	 * @param xmlQuery query in RCSB PDB XML format
	 * @throws IOException
	 */
	public AdvancedQuery(String xmlQuery) throws IOException {		
		AdvancedQueryService service = new AdvancedQueryService();
		List<String> results = service.postQuery(xmlQuery);
		
		entityLevel = results.size() > 0 && results.get(0).contains(":");
		structureIds = new HashSet<String>(results);
		this.exclusive = false; // exclusive not supported, query could match non-polymers
	}

	@Override
	public Boolean call(Tuple2<String, StructureDataInterface> t) throws Exception {
		StructureDataInterface structure = t._2;

		boolean globalMatch = false;
		int numChains = structure.getChainsPerModel()[0]; // only check first model
		int[] entityChainIndex = getChainToEntityIndex(structure);

		for (int i = 0; i < numChains; i++) {		
			String id = t._1;
			
			// match entity level information from RCSB Advanced search.
			// The results from the search are in the format <structureId>:<entityId>, e.g. 1F3M:1
			if (entityLevel) {
				id = getStructureEntityId(structure, id, entityChainIndex[i]);
			}

		    boolean match = structureIds.contains(id);

			if (match && ! exclusive) {
				return true;
			}
			if (! match && exclusive) {
				return false;
			}

			if (match) {
				globalMatch = true;
			}
		}

		return globalMatch;
	}

	private String getStructureEntityId(StructureDataInterface structure, String origStructureId, int origEntityId) {
		String id;
		
		// extract structure id from key
		String keyStructureId = origStructureId;
		int index = keyStructureId.indexOf(".");
		if (index > 0) {
		    keyStructureId = keyStructureId.substring(0, index);
		}
		
		int pos = structure.getStructureId().lastIndexOf(".");

		if (pos > 0) {
			// extracted structure id from value
			String valueStructureId = structure.getStructureId().substring(0, structure.getStructureId().indexOf(".")); 
			
			if (! keyStructureId.equals(valueStructureId)) {
				throw new IllegalArgumentException("Structures mismatch: key vs. value: " + 
						keyStructureId + " vs. " + valueStructureId);
			}

			// extract entity id
			String entityId = structure.getStructureId().substring(pos+1);
			
			// form composite id, which is used by the RCSB Advanced search
			id = valueStructureId + ":" + entityId; 
		} else {
			// use original entity id
		    id = keyStructureId + ":" + (origEntityId+1); // +1 since entity ids are one-based
		}
		
		return id;
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
