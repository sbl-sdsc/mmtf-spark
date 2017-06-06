/**
 * 
 */
package edu.sdsc.mmtf.spark.ml;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.Predictor;
import org.apache.spark.ml.feature.IndexToString;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * @author Peter Rose
 *
 */
public class SparkMultiClassClassifier {
	private Predictor predictor;
	private String label;
	private double testFraction = 0.3;
	private long seed = 1;

	public SparkMultiClassClassifier(Predictor predictor, String label, double testFraction, long seed) {
		this.predictor = predictor;
		this.label = label;
		this.testFraction = testFraction;
		this.seed = seed;
	}
	
	/**
	 * Dataset must at least contain the following two columns:
	 * label: the class labels
	 * features: feature vector
	 * @param data
	 * @return map with metrics
	 */
	public Map<String,String> fit(Dataset<Row> data) {
		int classCount = (int)data.select(label).distinct().count();
		
		StringIndexerModel labelIndexer = new StringIndexer()
		  .setInputCol(label)
		  .setOutputCol("indexedLabel")
		  .fit(data);

		// Split the data into training and test sets (30% held out for testing)
		Dataset<Row>[] splits = data.randomSplit(new double[] {1.0-testFraction, testFraction}, seed);
		Dataset<Row> trainingData = splits[0];
		Dataset<Row> testData = splits[1];

		// Set input columns
		predictor
		.setLabelCol("indexedLabel")
		.setFeaturesCol("features");

		// Convert indexed labels back to original labels.
		IndexToString labelConverter = new IndexToString()
		  .setInputCol("prediction")
		  .setOutputCol("predictedLabel")
		  .setLabels(labelIndexer.labels());

		// Chain indexers and forest in a Pipeline
		Pipeline pipeline = new Pipeline()
		  .setStages(new PipelineStage[] {labelIndexer, predictor, labelConverter});

		// Train model. This also runs the indexers.
		PipelineModel model = pipeline.fit(trainingData);

		// Make predictions.
		Dataset<Row> predictions = model.transform(testData).cache();
		
		// Display some sample predictions
		System.out.println();
		System.out.println("Sample predictions: " + predictor.getClass().getSimpleName());
		String primaryKey = predictions.columns()[0];
		predictions.select(primaryKey, label, "predictedLabel").sample(false, 0.1, seed).show(25);	
				
	
		predictions = predictions.withColumnRenamed(label, "stringLabel");
		predictions = predictions.withColumnRenamed("indexedLabel", label);
		
		// collect metrics
		Dataset<Row> pred = predictions.select("prediction",label);
        Map<String,String> metrics = new LinkedHashMap<>();       
        metrics.put("Method", predictor.getClass().getSimpleName());
        
        if (classCount == 2) {
        	    BinaryClassificationMetrics b = new BinaryClassificationMetrics(pred);
          	metrics.put("AUC", Float.toString((float)b.areaUnderROC()));
        }
	    
        MulticlassMetrics m = new MulticlassMetrics(pred); 
        metrics.put("F", Float.toString((float)m.weightedFMeasure()));
        metrics.put("Accuracy", Float.toString((float)m.accuracy()));
        metrics.put("Precision", Float.toString((float)m.weightedPrecision()));
        metrics.put("Recall", Float.toString((float)m.weightedRecall()));
        metrics.put("False Positive Rate", Float.toString((float)m.weightedFalsePositiveRate()));
        metrics.put("True Positive Rate", Float.toString((float)m.weightedTruePositiveRate()));

        return metrics;
	}
}
