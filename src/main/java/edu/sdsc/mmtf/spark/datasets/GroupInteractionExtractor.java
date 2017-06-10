/**
 * 
 */
package edu.sdsc.mmtf.spark.datasets;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.ml.JavaRDDToDataset;
import edu.sdsc.mmtf.spark.utils.StructureToAllInteractions;

/**
 * Creates a dataset of interactions of a specified group within
 * a cutoff distance. Groups are specified by there
 * Chemical Component identifier (residue name), e.g., "ZN", "ATP".
 *
 * @author Peter Rose
 *
 */
public class GroupInteractionExtractor {
	private String groupName;
	private double distance;

	/**
	 * @param groupName name of the group to be analyzed
	 * @param distance cutoff distance
	 */
	public GroupInteractionExtractor(String groupName, double distance) {
		this.groupName = groupName;
		this.distance = distance;
	}

	/**
	 * Returns a dataset of residues that interact with the specified group within
	 * a specified cutoff distance.
	 * @param structures
	 * @return dataset with interacting residue and atom information
	 */
	public Dataset<Row> getDataset(JavaPairRDD<String, StructureDataInterface> structures) {
	    // create a list of all residues with a threshold distance
	    JavaRDD<Row> rows = structures.flatMap(new StructureToAllInteractions(groupName, distance));
	   
        // convert to a dataset
	    return JavaRDDToDataset.getDataset(rows, "structureId","residue1","atom1","element1","index1",
	    		"residue2","atom2","element2","index2","distance");
	}
}
