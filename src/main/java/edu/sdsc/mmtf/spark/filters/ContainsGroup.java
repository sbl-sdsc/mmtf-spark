package edu.sdsc.mmtf.spark.filters;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.spark.api.java.function.Function;
import org.rcsb.mmtf.api.StructureDataInterface;

import scala.Tuple2;

/**
 * This filter returns true if structure contains specified groups.
 * Groups are specified by their one, two, and three-letter codes, e.g. "F", "MG", "ATP".
 * @author Peter Rose
 *
 */
public class ContainsGroup implements Function<Tuple2<String, StructureDataInterface>, Boolean> {
	private static final long serialVersionUID = -2195111374872792219L;
	private Set<String> groupQuery = null;

	public ContainsGroup(String...groups) {
		this.groupQuery = new HashSet<>(Arrays.asList(groups));
	}
	
	@Override
	public Boolean call(Tuple2<String, StructureDataInterface> t) throws Exception {
		StructureDataInterface structure = t._2;
	
		// find number of unique groups
		int[] groupIndices = structure.getGroupTypeIndices();
		int uniqueGroups = 0;
		for (int i = 0; i < structure.getGroupTypeIndices().length; i++) {
    		uniqueGroups = Math.max(uniqueGroups, groupIndices[i]);
    	}
		
		// add all groups into a set
		Set<String> groupNames = new HashSet<String>(uniqueGroups);
		for (int i = 0; i < uniqueGroups; i++) {
			groupNames.add(structure.getGroupName(i));
		}
		
		// check if groups in query are all present in the structure
		return groupNames.containsAll(groupQuery);
	}
}
