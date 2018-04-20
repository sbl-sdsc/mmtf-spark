package edu.sdsc.mmtf.spark.interactions;


import java.io.Serializable;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.rcsb.mmtf.api.StructureDataInterface;

/**
 * This class creates datasets of ligand - macromolecule and
 * macromolecule - macromolecule interaction information. 
 * Criteria to select interactions are specified by the 
 * {@link InteractionFilter}
 * 
 * @author Peter Rose
 * @since 0.2.0
 *
 */
public class InteractionFingerprinter implements Serializable {
	private static final long serialVersionUID = 7301359123881276464L;
	
	/**
	 * Returns a dataset of ligand - macromolecule interacting residues.
	 * 
	 * <p>The dataset contains the following columns:
	 * <pre>
	 *    structureChainId - pdbId.chainName of chain that interacts with ligand
     *    queryLigandId - id of ligand from PDB chemical component dictionary
     *    queryLigandNumber - group number of ligand including insertion code
     *    queryLigandChainId - chain name of ligand
     *    targetChainId - name of chain for which the interaction data are listed
     *    groupNumbers - array of residue numbers of interacting groups including insertion code (e.g., 101A)
     *    sequenceIndices - array of zero-based index of interaction groups (residues) mapped onto target sequence
     *    sequence - interacting polymer sequence
     *    interactingChains - total number of chains that interact with ligand
     * </pre>
	 * 
	 * @param structures a set of PDB structures
	 * @param filter interaction criteria
	 * @return dataset with interacting residue information
	 */
	public static Dataset<Row> getLigandPolymerInteractions(JavaPairRDD<String, StructureDataInterface> structures, InteractionFilter filter) {
	    // find all interactions
	    JavaRDD<Row> rows = structures.flatMap(new LigandInteractionFingerprint(filter));
	   
	    // convert RDD to a Dataset with the following columns
	    boolean nullable = false;
	    StructField[] fields = {
				DataTypes.createStructField("structureChainId", DataTypes.StringType, nullable),
				DataTypes.createStructField("queryLigandId", DataTypes.StringType, nullable),
                DataTypes.createStructField("queryLigandNumber", DataTypes.StringType, nullable),
                DataTypes.createStructField("queryLigandChainId", DataTypes.StringType, nullable),
                DataTypes.createStructField("targetChainId", DataTypes.StringType, nullable),
				DataTypes.createStructField("groupNumbers", DataTypes.createArrayType(DataTypes.StringType), nullable),
				DataTypes.createStructField("sequenceIndices", DataTypes.createArrayType(DataTypes.IntegerType), nullable),
				DataTypes.createStructField("sequence", DataTypes.StringType, nullable),
				DataTypes.createStructField("interactingChains", DataTypes.IntegerType, nullable)
		};
	    
	    SparkSession spark = SparkSession.builder().getOrCreate();
		return spark.createDataFrame(rows, new StructType(fields));
	}
	
	/**
     * Returns a dataset of ligand - macromolecule interaction information.
     *  * Criteria to select interactions are specified by the 
     * {@link InteractionFilter}
     * 
     * <p>The dataset contains the following columns:
     * <pre>
     *    structureChainId - pdbId.chainName for which the interaction data are listed
     *    queryChainId - name of chain that interacts with target chain
     *    targetChainId - name of chain for which the interaction data are listed
     *    groupNumbers - array of residue numbers of interacting groups including insertion code (e.g., 101A)
     *    sequenceIndices - array of zero-based index of interaction groups (residues) mapped onto target sequence
     *    sequence - target polymer sequence
     * </pre>
     *
     * @param structures a set of PDB structures
     * @param filter interaction criteria
     * @return dataset with interacting residue information
     */
    public static Dataset<Row> getPolymerInteractions(JavaPairRDD<String, StructureDataInterface> structures, InteractionFilter filter) {
        // find all interactions
        JavaRDD<Row> rows = structures.flatMap(new PolymerInteractionFingerprint(filter));
       
        // convert RDD to a Dataset with the following columns
        boolean nullable = false;
        StructField[] fields = {
                DataTypes.createStructField("structureChainId", DataTypes.StringType, nullable),
                DataTypes.createStructField("queryChainId", DataTypes.StringType, nullable),
                DataTypes.createStructField("targetChainId", DataTypes.StringType, nullable),
                DataTypes.createStructField("groupNumbers", DataTypes.createArrayType(DataTypes.StringType), nullable),
                DataTypes.createStructField("sequenceIndices", DataTypes.createArrayType(DataTypes.IntegerType), nullable), 
                DataTypes.createStructField("sequence", DataTypes.StringType, nullable)
        };
        
        SparkSession spark = SparkSession.builder().getOrCreate();
        return spark.createDataFrame(rows, new StructType(fields));
    }
}
