package edu.sdsc.mmtf.spark.ml;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
/**
 * Creates a balanced dataset for classification problems by either
 * downsampling the majority classes or upsampling the minority classes. 
 * It randomly samples each class and returns a dataset with approximately
 * the same number of samples in each class.
 * 
 * @author Peter Rose
 * @since 0.1.0
 *
 */
public class DatasetBalancer implements Serializable {
	private static final long serialVersionUID = -5377909570776485863L;

	/**
	 * Returns a balanced dataset for the given column name by
	 * downsampling the majority classes.
	 * The classification column must be of type String.
	 * 
	 * @param data dataset
	 * @param columnName column to be balanced by
	 * @param seed random number seed
	 * @return downsampled dataset
	 */
	public static Dataset<Row> downsample(Dataset<Row> data, String columnName, long seed) {
		// TODO specify max ratio between minority and majority class
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
	
	/**
	 * Returns a balanced dataset for the given column name by
	 * upsampling the minority classes.
	 * The classification column must be of type String.
	 * 
	 * @param data dataset
	 * @param columnName column to be balanced by
	 * @param seed random number seed
	 * @return upsampled dataset
	 */
	public static Dataset<Row> upsample(Dataset<Row> data, String columnName, long seed) {
		// TODO specify max ratio between minority and majority class
		List<Row> counts = data.groupBy(columnName).count().collectAsList();

		Long[] c = new Long[counts.size()];
		String[] names = new String[counts.size()];

		long maxCount = Long.MIN_VALUE;
		for (int i = 0; i < counts.size(); i++) {
			names[i] = counts.get(i).getString(0);
			c[i] = counts.get(i).getLong(1);
			maxCount = Math.max(maxCount, c[i]);
		}
		
		List<Dataset<Row>> samples = new ArrayList<>(counts.size());
		
		// random sample
		for (int i = 0; i < counts.size(); i++) {
			String condition = columnName + "='" + names[i] +"'";
			double fraction = maxCount/(double)c[i];
			if (Math.abs(1.0-fraction) > 1.0) {
				// upsample with replacement
			    samples.add(data.filter(condition).sample(true, fraction, seed));
			} else {
				samples.add(data.filter(condition));
			}
		}
		
		// create the balanced subset
		Dataset<Row> union = samples.get(0);
		for (int i = 1; i < counts.size(); i++) {
		    union = union.union(samples.get(i));
		}
		
		return union;
	}
}
