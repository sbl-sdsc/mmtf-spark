package edu.sdsc.mmtf.spark.ml;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

/**
 * This class contains methods for creating overlapping and
 * non-overlapping n-grams of one-letter code sequences 
 * (e.g., protein sequences).
 * 
 * @author Peter Rose
 *
 */
public class SequenceNgrammer {

	/**
	 * Splits a one-letter sequence column (e.g., protein sequence)
	 * into array of overlapping n-grams.
	 * 
	 * <p> Example 2-gram: IDCGH ... => [ID, DC, CG, GH, ...]
	 * 
	 * @param data input dataset with column "sequence"
	 * @param n size of the n-gram
	 * @return output dataset with appended ngram column
	 */
	public static Dataset<Row> ngram(Dataset<Row> data, int n, String outputCol) {
		SparkSession session = data.sparkSession();

		session.udf().register("encoder", new UDF1<String, String[]>() {
			private static final long serialVersionUID = 625555927535377578L;

			@Override
			public String[] call(String s) throws Exception {
				if (s == null) {
					return new String[0];
				}
				int t = s.length() - n + 1;
				String[] ngram = new String[t];
				for (int i = 0; i < t; i++) {
					ngram[i] = s.substring(i, i + n);
				}
				return ngram;
			}
		}, DataTypes.createArrayType(DataTypes.StringType));

		data.createOrReplaceTempView("table");

		// append ngram column
		return session.sql("SELECT *, encoder(sequence) AS "
				+ outputCol + " from table");
	}
	
	/**
	 * Splits a one-letter sequence column (e.g., protein sequence)
	 * into array of non-overlapping n-grams. To generate all possible n-grams,
	 * this method needs to be called n times with shift parameters {0, ..., n-1}.
	 * 
	 * <p> Example 3-gram(shift=0) : IDCGHTVEDQR ... => [IDC, GHT, VED, ...]
	 * <p> Example 3-gram(shift=1) : IDCGHTVEDQR ... => [DCG, HTV, EDQ, ...]
	 * <p> Example 3-gram(shift=2) : IDCGHTVEDQR ... => [CGH, TVE, DQR, ...]
	 * 
	 * <p>For an application of shifted n-grams see:
	 * E Asgari, MRK Mofrad, PLoS One. 2015; 10(11): e0141287, doi: 
	 * <a href="https://dx.doi.org/10.1371/journal.pone.0141287">10.1371/journal.pone.0141287</a>
     *
     * @param data input dataset with column "sequence"
     * @param n size of the n-gram
     * @param shift start index for the n-gram
     * @param outputCol name of the output column
     * @return output dataset with appended ngram column
     */
	public static Dataset<Row> shiftedNgram(Dataset<Row> data, int n, int shift, String outputCol) {
		SparkSession session = data.sparkSession();

		session.udf().register("encoder", new UDF1<String, String[]>() {
			private static final long serialVersionUID = 4844644794982507954L;

			@Override
			public String[] call(String s) throws Exception {
				if (shift > s.length()) {
					return new String[0];
				}
				s = s.substring(shift);
				int t = s.length() / n;
				
				String[] ngram = new String[t];

				for (int i = 0, j = 0; j < t; i += n) {
					ngram[j++] = s.substring(i, i + n);
				}
				return ngram;
			}
		}, DataTypes.createArrayType(DataTypes.StringType));

		data.createOrReplaceTempView("table");
		
		// append shifted ngram column
		return session.sql("SELECT *, encoder(sequence) AS " + outputCol + " from table");
	}
}
