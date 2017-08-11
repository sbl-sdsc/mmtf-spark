package edu.sdsc.mmtf.spark.ml;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.ml.feature.Word2Vec;
import org.apache.spark.ml.feature.Word2VecModel;
import org.apache.spark.ml.feature.Word2VecModel.Word2VecModelReader;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF3;

/**
 * This class encodes a protein sequence into a feature vector.
 * The protein sequence must be present in the input data set, 
 * the default column name is "sequence". The default column name
 * for the feature vector is "features".
 * 
 * @author Peter Rose
 *
 */
public class ProteinSequenceEncoder implements Serializable {
	private static final long serialVersionUID = 8420996420243386694L;
	private Dataset<Row> data;
	private String inputCol = "sequence";
	private String outputCol = "features";
	private Word2VecModel model;

	private static final List<Character> AMINO_ACIDS21 = Arrays.asList('A', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'K', 'L',
			'M', 'N', 'P', 'Q', 'R', 'S', 'T', 'V', 'W', 'X', 'Y');
	
	private static final Map<Character,double[]> properties = new HashMap<>(21);
	static {
		properties.put('A', new double[]{1.28,0.05,1.00,0.31,6.11,0.42,0.23});
		properties.put('G', new double[]{0.00,0.00,0.00,0.00,6.07,0.13,0.15});
		properties.put('V', new double[]{3.67,0.14,3.00,1.22,6.02,0.27,0.49});
		properties.put('L', new double[]{2.59,0.19,4.00,1.70,6.04,0.39,0.31});
		properties.put('I', new double[]{4.19,0.19,4.00,1.80,6.04,0.30,0.45});
		properties.put('F', new double[]{2.94,0.29,5.89,1.79,5.67,0.30,0.38});
		properties.put('Y', new double[]{2.94,0.30,6.47,0.96,5.66,0.25,0.41});
		properties.put('W', new double[]{3.21,0.41,8.08,2.25,5.94,0.32,0.42});
		properties.put('T', new double[]{3.03,0.11,2.60,0.26,5.60,0.21,0.36});
		properties.put('S', new double[]{1.31,0.06,1.60,-0.04,5.70,0.20,0.28});
		properties.put('A', new double[]{2.34,0.29,6.13,-1.01,10.74,0.36,0.25});
		properties.put('K', new double[]{1.89,0.22,4.77,-0.99,9.99,0.32,0.27});
		properties.put('H', new double[]{2.99,0.23,4.66,0.13,7.69,0.27,0.30});
		properties.put('D', new double[]{1.60,0.11,2.78,-0.77,2.95,0.25,0.20});
		properties.put('E', new double[]{1.56,0.15,3.78,-0.64,3.09,0.42,0.21});
		properties.put('N', new double[]{1.60,0.13,2.95,-0.60,6.52,0.21,0.22});
		properties.put('Q', new double[]{1.56,0.18,3.95,-0.22,5.65,0.36,0.25});
		properties.put('M', new double[]{2.35,0.22,4.43,1.23,5.71,0.38,0.32});
		properties.put('P', new double[]{2.67,0.00,2.72,0.72,6.80,0.13,0.34});
		properties.put('C', new double[]{1.77,0.13,2.43,1.54,6.35,0.17,0.41});
		properties.put('X', new double[]{0.00,0.00,0.00,0.00,0.00,0.00,0.00});
	}
	
	// Source: https://ftp.ncbi.nih.gov/repository/blocks/unix/blosum/BLOSUM/blosum62.blast.new
	private static final Map<Character,double[]> blosum62 = new HashMap<>(21);
	static {
	    //                             A  R  N  D  C  Q  E  G  H  I  L  K  M  F  P  S  T  W  Y  V   
	   blosum62.put('A', new double[]{ 4,-1,-2,-2, 0,-1,-1, 0,-2,-1,-1,-1,-1,-2,-1, 1, 0,-3,-2, 0});
	   blosum62.put('R', new double[]{-1, 5, 0,-2,-3, 1, 0,-2, 0,-3,-2, 2,-1,-3,-2,-1,-1,-3,-2,-3});
	   blosum62.put('N', new double[]{-2, 0, 6, 1,-3, 0, 0, 0, 1,-3,-3, 0,-2,-3,-2, 1, 0,-4,-2,-3}); 
	   blosum62.put('D', new double[]{-2,-2, 1, 6,-3, 0, 2,-1,-1,-3,-4,-1,-3,-3,-1, 0,-1,-4,-3,-3}); 
	   blosum62.put('C', new double[]{ 0,-3,-3,-3, 9,-3,-4,-3,-3,-1,-1,-3,-1,-2,-3,-1,-1,-2,-2,-1}); 
	   blosum62.put('Q', new double[]{-1, 1, 0, 0,-3, 5, 2,-2, 0,-3,-2, 1, 0,-3,-1, 0,-1,-2,-1,-2});
	   blosum62.put('E', new double[]{-1, 0, 0, 2,-4, 2, 5,-2, 0,-3,-3, 1,-2,-3,-1, 0,-1,-3,-2,-2});
	   blosum62.put('G', new double[]{ 0,-2, 0,-1,-3,-2,-2, 6,-2,-4,-4,-2,-3,-3,-2, 0,-2,-2,-3,-3});
	   blosum62.put('H', new double[]{-2, 0, 1,-1,-3, 0, 0,-2, 8,-3,-3,-1,-2,-1,-2,-1,-2,-2, 2,-3});
	   blosum62.put('I', new double[]{-1,-3,-3,-3,-1,-3,-3,-4,-3, 4, 2,-3, 1, 0,-3,-2,-1,-3,-1, 3});
	   blosum62.put('L', new double[]{-1,-2,-3,-4,-1,-2,-3,-4,-3, 2, 4,-2, 2, 0,-3,-2,-1,-2,-1, 1});
	   blosum62.put('K', new double[]{-1, 2, 0,-1,-3, 1, 1,-2,-1,-3,-2, 5,-1,-3,-1, 0,-1,-3,-2,-2});
	   blosum62.put('M', new double[]{-1,-1,-2,-3,-1, 0,-2,-3,-2, 1, 2,-1, 5, 0,-2,-1,-1,-1,-1, 1});
	   blosum62.put('F', new double[]{-2,-3,-3,-3,-2,-3,-3,-3,-1, 0, 0,-3, 0, 6,-4,-2,-2, 1, 3,-1});
	   blosum62.put('P', new double[]{-1,-2,-2,-1,-3,-1,-1,-2,-2,-3,-3,-1,-2,-4, 7,-1,-1,-4,-3,-2});
	   blosum62.put('S', new double[]{ 1,-1, 1, 0,-1, 0, 0, 0,-1,-2,-2, 0,-1,-2,-1, 4, 1,-3,-2,-2});
	   blosum62.put('T', new double[]{ 0,-1, 0,-1,-1,-1,-1,-2,-2,-1,-1,-1,-1,-2,-1, 1, 5,-2,-2, 0});
	   blosum62.put('W', new double[]{-3,-3,-4,-4,-2,-2,-3,-2,-2,-3,-2,-3,-1, 1,-4,-3,-2,11, 2,-3});
	   blosum62.put('Y', new double[]{-2,-2,-2,-3,-2,-1,-2,-3, 2,-1,-1,-2,-1, 3,-3,-2,-2, 2, 7,-1});
	   blosum62.put('V', new double[]{ 0,-3,-3,-3,-1,-2,-2,-3,-3, 3, 1,-2, 1,-1,-2,-2, 0,-3,-1, 4});
	   blosum62.put('X', new double[]{-4,-4,-4,-4,-4,-4,-4,-4,-4,-4,-4,-4,-4,-4,-4,-4,-4,-4,-4,-4});
	}
	
	public ProteinSequenceEncoder(Dataset<Row> data) {
		this.data = data;
	}

	public ProteinSequenceEncoder(Dataset<Row> data, String inputCol, String outputCol) {
		this.data = data;
		this.inputCol = inputCol;
		this.outputCol = outputCol;
	}
	
	/**
	 * One-hot encodes a protein sequence. The one-hot encoding
     * encodes the 20 natural amino acids, plus X for any other 
     * residue for a total of 21 elements per residue.
	 * 
	 * @return dataset with feature vector appended
	 */
	public Dataset<Row> oneHotEncode() {
		SparkSession session = data.sparkSession();

		session.udf().register("encoder", new UDF1<String, Vector>() {
			private static final long serialVersionUID = -6095318836772114908L;

			@Override
			public Vector call(String s) throws Exception {
				int len = AMINO_ACIDS21.size();

				double[] values = new double[len * s.length()];
				char[] seq = s.toCharArray();
				for (int i = 0; i < seq.length; i++) {
					int index = AMINO_ACIDS21.indexOf(seq[i]);
					// replace any non-matching code, e.g., U, with X
					if (index == -1) {
						index = AMINO_ACIDS21.indexOf('X');
					}
					values[i * len + index] = 1;
				}

				return Vectors.dense(values);
			}
		}, new VectorUDT());

		// append feature column
		data.createOrReplaceTempView("table");
		data = session.sql("SELECT *, encoder(" 
		+ inputCol + ") AS " 
				+ outputCol + " from table");
		
		return data;
	}

	/**
	 * Encodes a protein sequence by 7 physicochemical
	 * properties. 
	 * 
	 * <p> See:  Meiler, J., Müller, M., Zeidler, A. et al. J Mol Model (2001) 7: 360. doi:
	 * <a href="https://link.springer.com/article/10.1007/s008940100038">10.1007/s008940100038</a>
     *
	 * @return dataset with feature vector appended
	 */
	public Dataset<Row> propertyEncode() {
		SparkSession session = data.sparkSession();

		session.udf().register("encoder", new UDF1<String, Vector>(){
			private static final long serialVersionUID = 1L;

			@Override
			public Vector call(String s) throws Exception {
				double[] values = new double[7*s.length()];
				for (int i = 0, k = 0; i < s.length(); i++) {
					double[] property = properties.get(s.charAt(i));
					if (property != null) {
						for (double p: property) {
							values[k++] = p;
						}
					}	
				}
				return Vectors.dense(values);
			}
		}, new VectorUDT());

		// append feature column
				data.createOrReplaceTempView("table");
				data = session.sql("SELECT *, encoder(" 
				+ inputCol + ") AS " 
						+ outputCol + " from table");
				
				return data;
	}
	
	/**
	 * Encodes a protein sequence by a Blosum62 matrix.
	 * 
	 * <p> See: <a href="https://ftp.ncbi.nih.gov/repository/blocks/unix/blosum/BLOSUM/blosum62.blast.new">BLOSUM62 Matrix</a>
     *
	 * @return dataset with feature vector appended
	 */
	public Dataset<Row> blosum62Encode() {
		SparkSession session = data.sparkSession();

		session.udf().register("encoder", new UDF1<String, Vector>(){
			private static final long serialVersionUID = 1L;

			@Override
			public Vector call(String s) throws Exception {
				double[] values = new double[20*s.length()];
				for (int i = 0, k = 0; i < s.length(); i++) {
					double[] property = blosum62.get(s.charAt(i));
					if (property != null) {
						for (double p: property) {
							values[k++] = p;
						}
					}	
				}
				return Vectors.dense(values);
			}
		}, new VectorUDT());

		// append feature column
				data.createOrReplaceTempView("table");
				data = session.sql("SELECT *, encoder(" 
				+ inputCol + ") AS " 
						+ outputCol + " from table");
				
				return data;
	}
	
	/**
	 * Encodes a protein sequence by converting it into n-grams and
	 * then transforming it into a Word2Vec feature vector.
	 * 
	 * @param n
	 *            the number of words in an n-gram
	 * @param windowSize
	 *            width of the window used to slide across the sequence, context
	 *            words from [-window, window]).
	 * @param vectorSize
	 *            dimension of the feature vector
	 *            
	 * @return dataset with features vector added to original dataset
	 */
	public Dataset<Row> overlappingNgramWord2VecEncode(int n, int windowSize, int vectorSize) {
		// create n-grams out of the sequence
		// e.g., 2-gram IDCGH, ... => [ID, DC, CG, GH, ...
		data = SequenceNgrammer.ngram(data, n, "ngram");

		// convert n-grams to W2V feature vector
		// [ID, DC, CG, GH, ... => [0.1234, 0.2394, ...]
		Word2Vec word2Vec = new Word2Vec();
				word2Vec.setInputCol("ngram")
				.setOutputCol(outputCol)
				.setNumPartitions(8)
				.setWindowSize(windowSize)
				.setVectorSize(vectorSize);

		model = word2Vec.fit(data);
        data = model.transform(data);
        
		return data;
	}

	/**
	 * Encodes a protein sequence by converting it into n-grams and
	 * then transforming it using a pre-trained Word2Vec model read
	 * from a file.
	 * 
	 * @param fileName filename of Word2Vec model
	 * @param n size of sequence n-gram
	 * @return dataset with features vector added to original dataset
	 */
	public Dataset<Row> overlappingNgramWord2VecEncode(String fileName, int n) {
        Word2VecModelReader reader = new Word2VecModelReader();
        model = reader.load(fileName);
        
        System.out.println("model file  : " + fileName);
        System.out.println("  inputCol  : " + model.getInputCol());
        System.out.println("  windowSize: " + model.getWindowSize());
        System.out.println("  vectorSize: " + model.getVectorSize());
        
        data = SequenceNgrammer.ngram(data, n, "ngram");
       
        model.setOutputCol(outputCol);
        return model.transform(data);
	}

	/**
	 * Encodes a protein sequence as three non-overlapping 3-grams, 
	 * trains a Word2Vec model on the 3-grams, and then averages
	 * the three resulting feature vectors.
	 * 
	 * <P> Asgari E, Mofrad MRK (2015) Continuous Distributed Representation 
	 * of Biological Sequences for Deep Proteomics and Genomics. 
	 * PLOS ONE 10(11): e0141287. doi:
	 * <a href="https://doi.org/10.1371/journal.pone.0141287">10.1371/journal.pone.0141287</a>
     *
	 * @param windowSize
	 *            width of the window used to slide across the sequence, context
	 *            words from [-window, window]).
	 * @param vectorSize
	 *            dimension of the feature vector
	 *            
	 * @return dataset with features vector added to original dataset
	 */
	public Dataset<Row> shifted3GramWord2VecEncode(int windowSize, int vectorSize) {
		// create n-grams out of the sequence
		// e.g., 2-gram [IDCGH, ... => [ID, DC, CG, GH, ...
		// TODO set input column
		data = SequenceNgrammer.shiftedNgram(data, 3, 0, "ngram0");
		data = SequenceNgrammer.shiftedNgram(data, 3, 1, "ngram1");
		data = SequenceNgrammer.shiftedNgram(data, 3, 2, "ngram2");

		Dataset<Row> ngram0 = data.select("ngram0").withColumnRenamed("ngram0", "ngram");
		Dataset<Row> ngram1 = data.select("ngram1").withColumnRenamed("ngram1", "ngram");
		Dataset<Row> ngram2 = data.select("ngram2").withColumnRenamed("ngram2", "ngram");

		Dataset<Row> ngrams = ngram0.union(ngram1).union(ngram2);

		// convert n-grams to W2V feature vector
		// [I D, D C, C G, G H, ... => [0.1234, 0.2394, ...]
		Word2Vec word2Vec = new Word2Vec().setInputCol("ngram").setMinCount(10).setNumPartitions(8)
				.setWindowSize(windowSize).setVectorSize(vectorSize);

		model = word2Vec.fit(ngrams);

		model.setInputCol("ngram0");
		model.setOutputCol("features0");
		data = model.transform(data);

		model.setInputCol("ngram1");
		model.setOutputCol("features1");
		data = model.transform(data);

		model.setInputCol("ngram2");
		model.setOutputCol("features2");
		data = model.transform(data);

		data = averageFeatureVectors(data, outputCol);

		return data;
	}
	
	/**
	 * Encodes a protein sequence as three non-overlapping 3-grams, 
	 * then transforming it using a pre-trained Word2Vec model read
	 * from a file.
	 * 
	 * <P> Asgari E, Mofrad MRK (2015) Continuous Distributed Representation 
	 * of Biological Sequences for Deep Proteomics and Genomics. 
	 * PLOS ONE 10(11): e0141287. doi:
	 * <a href="https://doi.org/10.1371/journal.pone.0141287">10.1371/journal.pone.0141287</a>
     *
     * @param fileName 
     *            filename of Word2Vec model
	 * @param windowSize
	 *            width of the window used to slide across the sequence, context
	 *            words from [-window, window]).
	 * @param vectorSize
	 *            dimension of the feature vector
	 *            
	 * @return dataset with features vector added to original dataset
	 */
	public Dataset<Row> shifted3GramWord2VecEncode(String fileName) {
		 Word2VecModelReader reader = new Word2VecModelReader();
	     model = reader.load(fileName);
	        
	     System.out.println("model file  : " + fileName);
	     System.out.println("  inputCol  : " + model.getInputCol());
	     System.out.println("  windowSize: " + model.getWindowSize());
	     System.out.println("  vectorSize: " + model.getVectorSize());
		
		// create n-grams out of the sequence
		// e.g., 2-gram [IDCGH, ... => [ID, DC, CG, GH, ...
		// TODO set input column
		data = SequenceNgrammer.shiftedNgram(data, 3, 0, "ngram0");
		data = SequenceNgrammer.shiftedNgram(data, 3, 1, "ngram1");
		data = SequenceNgrammer.shiftedNgram(data, 3, 2, "ngram2");

		model.setInputCol("ngram0");
		model.setOutputCol("features0");
		data = model.transform(data);

		model.setInputCol("ngram1");
		model.setOutputCol("features1");
		data = model.transform(data);

		model.setInputCol("ngram2");
		model.setOutputCol("features2");
		data = model.transform(data);

		data = averageFeatureVectors(data, outputCol);

		return data;
	}
	
	/**
	 * Returns a Word2VecModel created by overlappingNgramWord2VecEncode().
     *
	 * @return overlapping Ngram Word2VecModel if available, otherwise null
	 */
	public Word2VecModel getWord2VecModel() {
		return model;
	}
	
	private static Dataset<Row> averageFeatureVectors(Dataset<Row> data, String outputCol) {
		SparkSession session = data.sparkSession();

		session.udf().register("averager", new UDF3<Vector, Vector, Vector, Vector>() {
			private static final long serialVersionUID = -8190379199020903671L;

			@Override
			public Vector call(Vector v1, Vector v2, Vector v3) throws Exception {
				double[] f1 = v1.toArray();
				double[] f2 = v2.toArray();
				double[] f3 = v3.toArray();
				
				// arrays may be of different length
				int len = Math.min(Math.min(f1.length, f2.length), f3.length);
				double[] average = new double[len];

				for (int i = 0; i < len; i++) {
					average[i] = (f1[i] + f2[i] + f3[i]) / 3.0;
				}
				return Vectors.dense(average);
			}
		}, new VectorUDT());

		data.createOrReplaceTempView("table");
		// append new feature column with average values
		return session.sql("SELECT *, averager(features0,features1,features2) AS " + outputCol + " from table");
	}
}
