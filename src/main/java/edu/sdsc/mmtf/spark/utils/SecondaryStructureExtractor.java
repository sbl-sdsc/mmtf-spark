package edu.sdsc.mmtf.spark.utils;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.rcsb.mmtf.api.StructureDataInterface;

import scala.Tuple2;

public class SecondaryStructureExtractor {

	public static Dataset<Row> getAsDataset(JavaPairRDD<String, StructureDataInterface> structure) {
		SparkSession spark = SparkSession.builder().getOrCreate();

		StructType schema = new StructType(new StructField[] {
				DataTypes.createStructField("structureChainId", DataTypes.StringType, false),
				DataTypes.createStructField("sequence", DataTypes.StringType, false),
				DataTypes.createStructField("alpha", DataTypes.FloatType, false),
				DataTypes.createStructField("beta", DataTypes.FloatType, false),
				DataTypes.createStructField("coil", DataTypes.FloatType, false)
		});

		JavaRDD<Row> rows = structure.map(t -> getSecStructFractions(t));

		return spark.createDataFrame(rows, schema);
	}

	private static Row getSecStructFractions(Tuple2<String, StructureDataInterface> t) {
		String key = t._1;
		StructureDataInterface structure = t._2;
		
		float helix = 0;
		float sheet = 0;
		float coil = 0;

		for (int code: structure.getSecStructList()) {
			switch (DsspSecondaryStructure.getQ3Code(code)) {

			case ALPHA_HELIX:
				helix++;
				break;
			case EXTENDED:
				sheet++;
				break;
			case COIL:
				coil++;
				break;
			default:
				break;
			}
		}
		helix /= structure.getSecStructList().length;
		sheet /= structure.getSecStructList().length;
		coil /= structure.getSecStructList().length;	

		return RowFactory.create(key, structure.getEntitySequence(0), helix, sheet, coil);
	}
}
