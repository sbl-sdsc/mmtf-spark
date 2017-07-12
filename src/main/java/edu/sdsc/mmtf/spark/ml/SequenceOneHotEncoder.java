package edu.sdsc.mmtf.spark.ml;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;

/**
 * This class adds a One-hot encoded feature vector column ("feature") 
 * to a dataset that contains a "sequence" column. The one-hot encoding
 * encodes the 20 natural amino acids, plus X for any other residue for
 * a total of 21 elements per residue.
 * 
 * @author Peter Rose
 *
 */
public class SequenceOneHotEncoder {
	private static final List<Character> AMINO_ACIDS21 = 
			Arrays.asList('A','C','D','E','F','G','H','I','K','L','M','N','P','Q','R','S','T','V','W','X','Y');

	public static Dataset<Row> encode(Dataset<Row> data) {
		SparkSession session = data.sparkSession();
		
		session.udf().register("oneHotEncoder", new UDF1<String, Vector>(){
			private static final long serialVersionUID = 1L;

			@Override
			public Vector call(String s) throws Exception {
				int len = AMINO_ACIDS21.size();

				double[] values = new double[len*s.length()];
				char[] seq = s.toCharArray();
				for (int i = 0; i < seq.length; i++) {
					int index = AMINO_ACIDS21.indexOf(seq[i]);
					// replace any non-matching code, e.g., U, with X
					if (index == -1) {
						index = AMINO_ACIDS21.indexOf('X');
					}
					values[i*len+index] = 1;
				}

				return Vectors.dense(values);
			}	
		}, new VectorUDT());
		
		data.createOrReplaceTempView("table");
		// append one-hot encoded sequence as a new column "features"
		return session.sql("SELECT *, oneHotEncoder(sequence) AS features from table");
	}
}
