package edu.sdsc.mmtf.spark.datasets;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.mappers.StructureToSecondaryStructureElements;
import edu.sdsc.mmtf.spark.ml.JavaRDDToDataset;

/**
 * TODO
 * @author Yue Yu
 * @since 0.1.0
 */
public class SecondaryStructureElementExtractor {

	/**
	 * TODO
	 */
	public static Dataset<Row> getDataset(JavaPairRDD<String, StructureDataInterface> structureRDD, String label) {
	
		JavaRDD<Row> rows =  SecondaryStructureExtractor.getJavaRDD(structureRDD)
				.flatMap(new StructureToSecondaryStructureElements(label));

		return JavaRDDToDataset.getDataset(rows, "sequence", "label");
	}
	
	/**
	 * TODO
	 */
	public static Dataset<Row> getDataset(JavaPairRDD<String, StructureDataInterface> structureRDD, String label, int length) {
	
		JavaRDD<Row> rows =  SecondaryStructureExtractor.getJavaRDD(structureRDD)
				.flatMap(new StructureToSecondaryStructureElements(label, length));

		return JavaRDDToDataset.getDataset(rows, "sequence", "label");
	}
}
