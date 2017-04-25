package edu.sdsc.mmtf.spark.mappers;

/**
 * Class to store summary data about a structure.
 * Using boxed types so they can be null
 * @author Anthony Bradley
 *
 */
public class SummaryData {

	/** The number of bonds in the structure.*/
	public Integer numBonds;
	/** The number of atoms in the structure.*/
	public Integer numAtoms;
	/** The number of groups in the structure.*/
	public Integer numGroups;
	/** The number of chains in the structure.*/
	public Integer numChains;
	/** The number of models in the structure.*/
	public Integer numModels;
	
	public String toString() {
		return "bonds=" + numBonds + ", atoms=" + numAtoms + ",groups=" + numGroups + ", chains=" + numChains + ",models=" + numModels;
	}
}
