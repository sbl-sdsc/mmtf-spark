package edu.sdsc.mmtf.spark.ml;

import java.io.Serializable;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.feature.NGram;
import org.apache.spark.ml.feature.RegexTokenizer;
import org.apache.spark.ml.feature.Word2Vec;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class SequenceWord2Vector implements Serializable {
	private static final long serialVersionUID = -4917145846599991881L;

	/**
	 * Adds a feature vector "features" to the dataset by converting a one-letter
	 * sequence "sequence" into n-grams and then into Word2Vectors.
	 * 
	 * @param data input dataset with "sequence" column
	 * @param n the number of words in an n-gram
	 * @param windowSize width of the window used to slide across the sequence
	 * @param vectorSize dimension of the feature vector
	 * @return dataset with "features" vector added to original dataset
	 */
	public static Dataset<Row> addFeatureVector(Dataset<Row> data, int n, int windowSize, int vectorSize) {

		// split sequence into an array of one-letter "words"
		// e.g. IDCGH... => [I, D, C, G, H, ...
		RegexTokenizer tokenizer = new RegexTokenizer()
				.setInputCol("sequence")
				.setOutputCol("words")
				.setToLowercase(false)
		  	    .setPattern("");
		
		// create n-grams out of the sequence
		// e.g., 2-gram [I, D, C, G, H, ... => [I D, D C, C G, G H, ...
		NGram ngrammer = new NGram()
				.setN(n)
				.setInputCol("words")
				.setOutputCol("ngram");

		// convert n-grams to W2V feature vector
		// [I D, D C, C G, G H, ... => [0.1234, 0.2394, ...]
		Word2Vec word2Vec = new Word2Vec()
				.setInputCol("ngram")
				.setOutputCol("features")
				.setMinCount(10)
				.setWindowSize(windowSize)
				.setVectorSize(vectorSize);

		Pipeline pipeline = new Pipeline().setStages(new PipelineStage[] {tokenizer, ngrammer, word2Vec});

		PipelineModel model = pipeline.fit(data);

		return model.transform(data);
	}
}
