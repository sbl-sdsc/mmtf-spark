package edu.sdsc.mmtf.spark.datasets;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.mappers.SequenceToSegment;
import edu.sdsc.mmtf.spark.ml.JavaRDDToDataset;

/**
 * TODO
 * @author Yue Yu
 *
 */
public class SequenceSegmentsExtractor {

	public static Dataset<Row> getDataset(JavaPairRDD<String, StructureDataInterface> structureRDD) {

		JavaRDD<Row> rows =  SecondaryStructureExtractor.getJavaRDD(structureRDD)
				.flatMap(new SequenceToSegment());
		
		return JavaRDDToDataset.getDataset(rows, "structureChainId","sequence", "labelQ8", "labelQ3");
	}
	
	public static Dataset<Row> getDataset(JavaPairRDD<String, StructureDataInterface> structureRDD, int length) {

		JavaRDD<Row> rows =  SecondaryStructureExtractor.getJavaRDD(structureRDD)
				.flatMap(new SequenceToSegment(length));
		
		return JavaRDDToDataset.getDataset(rows, "structureChainId","sequence", "labelQ8", "labelQ3");
	}
}
