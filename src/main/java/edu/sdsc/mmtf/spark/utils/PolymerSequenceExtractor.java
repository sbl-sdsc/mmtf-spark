package edu.sdsc.mmtf.spark.utils;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.mappers.StructureToPolymerSequences;
import edu.sdsc.mmtf.spark.ml.JavaRDDToDataset;

public class PolymerSequenceExtractor {

	public static Dataset<Row> getDataset(JavaPairRDD<String, StructureDataInterface> structureRDD) {
		JavaRDD<Row> rows = structureRDD
				.flatMapToPair(new StructureToPolymerSequences())
				.map(t -> RowFactory.create(t._1, t._2));
		
		return JavaRDDToDataset.getDataset(rows, "structureChainId","sequence");
	}
}
