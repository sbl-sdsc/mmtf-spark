package edu.sdsc.mmtf.spark.datasets;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.mappers.StructureToSecondaryStructureSegments;
import edu.sdsc.mmtf.spark.ml.JavaRDDToDataset;

/**
 * This class creates a dataset of sequence segments of specified length
 * and associated secondary structure information. Sequence and 
 * secondary structure strings are split into segments using a sliding 
 * window of the specified segment length. The dataset contains the 
 * sequence segment and the DSSP Q8 and DSSP Q3 secondary structure
 * annotation of the center residue. Therefore, the segment length
 * must be an odd number.
 * 
 * @author Yue Yu
 * @since 0.1.0
 *
 */
public class SecondaryStructureSegmentExtractor {

	/**
	 * Returns a dataset of sequence segments of the specified length and 
	 * the DSSP Q8 and DSSP Q3 code of the center residue in a segment.
	 * 
	 * @param structureRDD structure
	 * @param length segment length, must be an odd number
	 * @return dataset of segments
	 */
	public static Dataset<Row> getDataset(JavaPairRDD<String, StructureDataInterface> structureRDD, int length) {
		if (length % 2 == 0) {
			throw new IllegalArgumentException("segment length must be an odd number: " + length);
		}
		
		JavaRDD<Row> rows =  SecondaryStructureExtractor.getJavaRDD(structureRDD)
				.flatMap(new StructureToSecondaryStructureSegments(length));

		return JavaRDDToDataset.getDataset(rows, "structureChainId","sequence", "labelQ8", "labelQ3");
	}
}
