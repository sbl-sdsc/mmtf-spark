/**
 * 
 */
package edu.sdsc.mmtf.spark.incubator;

import java.io.IOException;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.feature.NGram;
import org.apache.spark.ml.feature.RegexTokenizer;
import org.apache.spark.ml.feature.Word2Vec;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import edu.sdsc.mmtf.spark.io.MmtfReader;
import edu.sdsc.mmtf.spark.mappers.StructureToPolymerChains;
import edu.sdsc.mmtf.spark.rcsbfilters.BlastClusters;
import edu.sdsc.mmtf.spark.webservices.CustomReportService;

/**
 * @author peter
 *
 */
public class CathClassificationDataset {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {

	    if (args.length != 2) {
	        System.err.println("Usage: " + CathClassificationDataset.class.getSimpleName() + " <hadoop sequence file> <output file>");
	        System.exit(1);
	    }
	    
	    long start = System.nanoTime();
	    
	    Dataset<Row> sequenceData = getSequenceData(args); 
        Dataset<Row> data = getCathLevel1();
//        Dataset<Row> data = getEcClassificationLevel1();
        
        // Join Cath data with sequence data
        data = data.join(sequenceData, "structureChainID").cache();	    
	    data.show(10);
	    
		int n = 2;
		int windowSize = 25;
		int vectorSize = 50;		
		data = sequenceToFeatureVector(data, n, windowSize, vectorSize).cache();

		System.out.println("Dataset size: " + data.count());		
		data.show(10);
		
        data = data.select("structureChainId", "label", "features");
	
        data.write().mode("overwrite").format("parquet").save(args[1]);
		
		long end = System.nanoTime();

		System.out.println((end-start)/1E9 + " sec");
	}

	private static Dataset<Row> getSequenceData(String[] args) throws IOException {
	    SparkSession spark = SparkSession
		.builder()
		.master("local[*]")
		.getOrCreate();
		
	    JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
	    
		JavaRDD<Row> pdb = MmtfReader
				.readSequenceFile(args[0], sc)
				.filter(new BlastClusters(40))
				.flatMapToPair(new StructureToPolymerChains())
				.filter(new BlastClusters(40))
				.map(t -> RowFactory.create(t._1, t._2.getEntitySequence(0)));
        
        // Generate the schema
        StructType schema = new StructType(new StructField[]{
        		new StructField("structureChainId", DataTypes.StringType, false, Metadata.empty()),
        		new StructField("sequence", DataTypes.StringType, false, Metadata.empty())
        });

        // Apply the schema to the RDD
        return spark.createDataFrame(pdb, schema);
	}
	
	private static Dataset<Row> sequenceToFeatureVector(Dataset<Row> data, int n, int windowSize, int vectorSize) {

		// split sequence into an array of one-letter codes (1-grams)
		// e.g. IDCGHVDSL => [i, d, c, g, h, v...
		RegexTokenizer tokenizer = new RegexTokenizer()
				.setInputCol("sequence")
				.setOutputCol("1gram")
				.setPattern("(?!^)"); 

		// create n-grams out of the sequence
		// e.g., 2-gram [i, d, c, g, h, v... => [i d, d c, c g, g...
		NGram ngrammer = new NGram()
				.setN(n)
				.setInputCol("1gram")
				.setOutputCol("ngram");

		// convert n-grams to W2V feature vector
		// [i d, d c, c g, g... => [0.1234, 0.23948, ...]
		Word2Vec word2Vec = new Word2Vec()
				.setInputCol("ngram")
				.setOutputCol("features")
				.setWindowSize(windowSize)
				.setVectorSize(vectorSize)
				.setMinCount(0);

		Pipeline pipeline = new Pipeline()
				.setStages(new PipelineStage[] {tokenizer, ngrammer, word2Vec});
//		.setStages(new PipelineStage[] {tokenizer, word2Vec});

		PipelineModel model = pipeline.fit(data);

		data = model.transform(data);

		return data;
	}


	private static Dataset<Row> getCathLevel1() throws IOException {
		Dataset<Row> data = CustomReportService.getDataset("cathId"); // this comes from _entity.pdbx_ec!
        data.show(5);
		data.createOrReplaceTempView("table");	
        
		String sql = "SELECT structureChainId, SUBSTRING_INDEX(cathId, '.', 1) as label from table " +
		"WHERE cathId IS NOT NULL AND cathId NOT LIKE '%#%'";
		
		data = data.sparkSession().sql(sql);
		data.show(20);
		
		return data;
	}

	private static Dataset<Row> getScopFold() throws IOException {
		Dataset<Row> ecData = CustomReportService.getDataset("scopFold","rankNumber90"); // this comes from _entity.pdbx_ec!
 //       ecData = ecData.dropDuplicates("structureId", "taxonomyId");
        
        // prepare data
        // 1. keep structureChainID as primary key
        // 2. create column label
        // 4. exclude records that contain the "#", which delimits multiple data
        // 5. use only first representative in 90% sequence identity cluster
        ecData.createOrReplaceTempView("table");	
        
		String sql = "SELECT structureChainId, scopFold as label from table " +
		"WHERE scopFold IS NOT NULL AND scopFold NOT LIKE '%#%' AND rankNumber90 ='1'";
		
		ecData = ecData.sparkSession().sql(sql);
		ecData.show(20);
		
		return ecData;
	}
	
	private static Dataset<Row> getEcClassificationLevel1() throws IOException {
		Dataset<Row> data = CustomReportService.getDataset("ecNo"); // this comes from _entity.pdbx_ec!
        
        // prepare data
        // 1. keep structureChainID as primary key
        // 2. create column ecLevel1 with only the top level EC number
        // 3. exclude records where the ecNo is null
        // 4. exclude records that contain the "#", which delimits multiple EC numbers
        // 5. use only first representative in 90% sequence identity cluster
        data.createOrReplaceTempView("table");	
        
		String sql = "SELECT structureChainId, SUBSTRING_INDEX(ecNo, '.', 1) as label from table " +
		"WHERE ecNo IS NOT NULL AND ecNo NOT LIKE '%#%'";
		
		data = data.sparkSession().sql(sql);
		data.show(20);
		
		return data;
	}
	
}
