package edu.sdsc.mmtf.spark.datasets;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.mappers.StructureToSecondaryStructureElements;
import edu.sdsc.mmtf.spark.ml.JavaRDDToDataset;

/**
 * Returns a datset of continuous segments of protein sequence with the specified
 * DSSP secondary structure code (E, H, C) of a minimum length.
 * <p>Example
 * <pre>
 * +-------------+-----+
 * |sequence     |label|
 * +-------------+-----+
 * |TFIVTA       |E    |
 * |ALTGTYE      |E    |
 * </pre>
 * 
 * @author Yue Yu
 * @since 0.1.0
 */
public class SecondaryStructureElementExtractor {
	

	 /**
	  * Returns a datset of continuous segments of protein sequence with the specified
	  * DSSP secondary structure code (E, H, C) of a minimum length.
	  * @param structure structure data
	  * @param label DSSP secondary structure label (E, H, C)
	  * @param length minimum length of secondary structure segment
	  * @return
	  */
	public static Dataset<Row> getDataset(JavaPairRDD<String, StructureDataInterface> structure, String label, int length) {
	
		JavaRDD<Row> rows =  SecondaryStructureExtractor.getJavaRDD(structure)
				.flatMap(new StructureToSecondaryStructureElements(label, length));

		return JavaRDDToDataset.getDataset(rows, "sequence", "label");
	}
}
