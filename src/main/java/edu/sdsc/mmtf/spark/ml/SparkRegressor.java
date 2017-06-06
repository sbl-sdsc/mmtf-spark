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
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * @author Peter Rose
 *
 */
public class SparkRegressor {
	private Predictor predictor;
	private String label;
	private double testFraction = 0.3;
	private long seed = 1;

	public SparkRegressor(Predictor predictor, String label, double testFraction, long seed) {
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

		// Split the data into training and test sets (30% held out for testing)
		Dataset<Row>[] splits = data.randomSplit(new double[] {1.0-testFraction, testFraction}, seed);
		Dataset<Row> trainingData = splits[0];
		Dataset<Row> testData = splits[1];

		// Train a RandomForest model.
		predictor
		  .setLabelCol(label)
		  .setFeaturesCol("features");

		// Chain indexer and forest in a Pipeline
		Pipeline pipeline = new Pipeline()
		  .setStages(new PipelineStage[] {predictor});

		// Train model. This also runs the indexer.
		PipelineModel model = pipeline.fit(trainingData);

		// Make predictions.
		Dataset<Row> predictions = model.transform(testData);

		// Display some sample predictions
		System.out.println("Sample predictions: " + predictor.getClass().getSimpleName());
		String primaryKey = predictions.columns()[0];
		predictions.select(primaryKey, label, "prediction").sample(false, 0.1, seed).show(50);
		
		Map<String,String> metrics = new LinkedHashMap<>();
	        
	    metrics.put("Method", predictor.getClass().getSimpleName());
	    
	    // Select (prediction, true label) and compute test error
	    RegressionEvaluator evaluator = new RegressionEvaluator()
	 		  .setLabelCol(label)
	 		  .setPredictionCol("prediction")
	 		  .setMetricName("rmse");
	    
	    metrics.put("rmse", Double.toString(evaluator.evaluate(predictions)));

		return metrics;
	}
}
