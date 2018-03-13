package edu.sdsc.mmtf.spark.interactions;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.utils.AtomInteraction;
import edu.sdsc.mmtf.spark.utils.StructureToAtomInteractions;


/**
 * Creates a dataset of noncovalent interactions of specified groups (residues)
 * in macromolecular structures. The criteria for interactions are specified
 * using an {@link InteractionFilter}. The interactions can be returned as 
 * interacting atom pairs or as one row per interacting atom.
 * 
 * <p> Typical use cases include:
 * <ul>
 * <li> Find interactions between a metal ion and protein/DNA/RNA
 * <li> Find interactions between a small molecule and protein/DNA/RNA
 * </ul>
 *
 * @author Peter Rose
 * @since 0.2.0
 *
 */
public class GroupInteractionExtractor {
	
	/**
	 * Returns a Dataset of pairwise interactions that satisfy the criteria of
	 * the {@link InteractionFilter}. Each atom, its interacting neighbor atom, and 
	 * the interaction distance is represented as a row.
	 * 
	 * @param structures a set of PDB structures
	 * @return filter criteria for determining noncovalent interactions
	 * @see edu.sdsc.mmtf.spark.interactions.InteractionFilter
	 */
	public static Dataset<Row> getPairInteractions(JavaPairRDD<String, StructureDataInterface> structures, InteractionFilter filter) {
		SparkSession spark = SparkSession.builder().getOrCreate();	
		@SuppressWarnings("resource") // sc cannot be closed here, it's still required elsewhere
		JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

	    // calculate interactions
		boolean pairwise = true;
		JavaRDD<Row> rows = structures.flatMap(new StructureToAtomInteractions(sc.broadcast(filter), pairwise));
		
		// convert JavaRDD to Dataset
		return spark.createDataFrame(rows, AtomInteraction.getPairInteractionSchema());
	}
	
	/**
     * Returns a dataset of interactions that satisfy the criteria of
     * the {@link InteractionFilter}. Each atom and its interacting neighbor atoms
     * are represented as a row in a Dataset. In addition, geometric features 
     * of the interactions, such as distances, angles, and orientational order 
     * parameters are returned in each row (see {@link edu.sdsc.mm.dev.utils.CoordinationGeometry}).
     * 
     * @param structures a set of PDB structures
     * @return filter criteria for determining noncovalent interactions
     * @see edu.sdsc.mmtf.spark.interactions.InteractionFilter
     * @see edu.sdsc.mm.dev.utils.CoordinationGeometry
	 */
	public static Dataset<Row> getInteractions(JavaPairRDD<String, StructureDataInterface> structures, InteractionFilter filter) {
		SparkSession spark = SparkSession.builder().getOrCreate();
		@SuppressWarnings("resource") // sc cannot be closed here, it's still required elsewhere
		JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

		// calculate interactions
		boolean pairwise = false;
		JavaRDD<Row> rows = structures.flatMap(new StructureToAtomInteractions(sc.broadcast(filter), pairwise));
		
		// convert JavaRDD to Dataset
		return spark.createDataFrame(rows, AtomInteraction.getSchema(filter.getMaxInteractions()));
	}
}
