package edu.sdsc.mmtf.spark.ml;

import java.io.Serializable;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * This class converts a JavaRDD<Row> to a Dataset<Row>. This method only
 * supports simple data types and all data need to be not null.
 * @author Peter Rose
 *
 */
public class JavaRDDToDataset implements Serializable {
	private static final long serialVersionUID = 4930832444212067031L;

	/**
	 * Converts a JavaRDD<Row> to a Dataset<Row>. This method only
	 * supports simple data types and all data need to be not null.
	 * 
	 * @param data JavaRDD of Row objects
	 * @param colNames names of the columns in a row
	 * @return
	 */
	public static Dataset<Row> getDataset(JavaRDD<Row> data, String...colNames) {
		// create the schema for the dataset
		Row row = data.first();
		int length = row.length();
		
		if (length != colNames.length) {
			throw new IllegalArgumentException("colNames length does not match row length");
		}
		
		StructField[] sf = new StructField[length];
		
		for (int i = 0; i < row.size(); i++) {
			Object o = row.get(i);

			// TODO add more types
			if (o instanceof String) {
				sf[i] = DataTypes.createStructField(colNames[i], DataTypes.StringType, false);
			} else if (o instanceof Integer) {
				sf[i] = DataTypes.createStructField(colNames[i], DataTypes.IntegerType, false);
			} else if (o instanceof Float) {
				sf[i] = DataTypes.createStructField(colNames[i], DataTypes.FloatType, false);
			} else if (o instanceof Double) {
				sf[i] = DataTypes.createStructField(colNames[i], DataTypes.DoubleType, false);
			} else {
				System.out.println("Data type not implemented yet");
			}
		}
		StructType schema = new StructType(sf);

		// convert JavaRDD to Dataset
		SparkSession spark = SparkSession.builder().getOrCreate();
		return spark.createDataFrame(data, schema);
	}
}
