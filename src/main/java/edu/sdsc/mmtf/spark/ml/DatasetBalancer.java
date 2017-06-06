package edu.sdsc.mmtf.spark.ml;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
/**
 * This class creates a balanced dataset for classification problems.
 * It randomly samples each class and return a dataset with approximately
 * the same number of samples in each class.
 * 
 * @author Peter Rose
 *
 */
public class DatasetBalancer {

	/**
	 * Returns a balanced dataset for the given column name.
	 * The classification column must be of type String.
	 * 
	 * @param data dataset
	 * @param columnName column to be balanced by
	 * @param seed random number seed
	 * @return
	 */
	public static Dataset<Row> balance(Dataset<Row> data, String columnName, long seed) {
		List<Row> counts = data.groupBy(columnName).count().collectAsList();

		Long[] c = new Long[counts.size()];
		String[] names = new String[counts.size()];

		long minCount = Long.MAX_VALUE;
		for (int i = 0; i < counts.size(); i++) {
			names[i] = counts.get(i).getString(0);
			c[i] = counts.get(i).getLong(1);
			minCount = Math.min(minCount, c[i]);
		}
		
		List<Dataset<Row>> samples = new ArrayList<>(counts.size());
		
		// random sample
		for (int i = 0; i < counts.size(); i++) {
			String condition = columnName + "='" + names[i] +"'";
			double fraction = minCount/(double)c[i];
			samples.add(data.filter(condition).sample(false, fraction, seed));
		}
		
		// create the balanced subset
		Dataset<Row> union = samples.get(0);
		for (int i = 1; i < counts.size(); i++) {
		    union = union.union(samples.get(i));
		}
		
		return union;
	}
}
