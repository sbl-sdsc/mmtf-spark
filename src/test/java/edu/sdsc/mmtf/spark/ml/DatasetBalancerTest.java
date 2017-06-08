package edu.sdsc.mmtf.spark.ml;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

public class DatasetBalancerTest {
	@Test
	public void test() {
		List<Row> rows = Arrays.asList(
				RowFactory.create("a", 1), RowFactory.create("a", 2), 
				RowFactory.create("b", 1), RowFactory.create("b", 2), RowFactory.create("b", 3), 
				RowFactory.create("c", 1), RowFactory.create("c", 2), RowFactory.create("c", 3), RowFactory.create("c", 4));

		SparkSession spark = SparkSession.builder().master("local[1]").getOrCreate();

		StructType schema = new StructType(
				new StructField[] { DataTypes.createStructField("key", DataTypes.StringType, false),
						DataTypes.createStructField("value", DataTypes.IntegerType, false) });

		Dataset<Row> data = spark.createDataFrame(rows, schema);

		long seed = 19;
		Dataset<Row> balancedData = DatasetBalancer.downsample(data, "key", seed);
		balancedData.show(9);
		assertTrue(balancedData.count() > 0);
		
	    spark.close();
	}

}
