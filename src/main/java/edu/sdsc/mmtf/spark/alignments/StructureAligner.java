package edu.sdsc.mmtf.spark.alignments;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.vecmath.Point3d;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.mappers.StructuralAlignmentMapper;
import edu.sdsc.mmtf.spark.utils.ColumnarStructureX;
import scala.Tuple2;

/**
 * This class performs parallel structure alignments. It performs
 * all vs. all and query set vs. all alignments.
 * 
 * @author Peter Rose
 * @since 0.2.0
 *
 */
public class StructureAligner implements Serializable {
	private static final long serialVersionUID = -7649106216436396239L;
	private static int NUM_TASKS = 3; // number of tasks per partition, Spark doc. suggests to use around 3.

	/**
	 * Calculates all vs. all structural alignments of protein chains using the 
	 * specified alignment algorithm. The input structures must contain single 
	 * protein chains.
	 * 
	 * @param targets structures containing single protein chains
	 * @param alignmentAlgorithm name of the algorithm
	 * @return dataset with alignment metrics
	 */
	public static Dataset<Row> getAllVsAllAlignments(JavaPairRDD<String, StructureDataInterface> targets,
			String alignmentAlgorithm) {

		SparkSession session = SparkSession.builder().getOrCreate();
		JavaSparkContext sc = new JavaSparkContext(session.sparkContext());

		// create a list of chainName/ C Alpha coordinates
		List<Tuple2<String, Point3d[]>> chains  = targets.mapValues(
				s -> new ColumnarStructureX(s,true).getcAlphaCoordinates()).collect();

		// create an RDD of all pair indices (0,1), (0,2), ..., (1,2), (1,3), ...
		JavaRDD<Tuple2<Integer, Integer>> pairs = getPairs(sc, chains.size());
		
		// calculate structural alignments for all pairs.
		// broadcast (copy) chains to all worker nodes for efficient processing.
		// for each pair there can be zero or more solutions, therefore we flatmap the pairs.
		JavaRDD<Row> rows = pairs.flatMap(new StructuralAlignmentMapper(sc.broadcast(chains), alignmentAlgorithm));

		// convert rows to a dataset
		return session.createDataFrame(rows, getSchema());
	}

	/**
	 * Calculates structural alignments between a query and a target set of protein chains 
	 * using the specified alignment algorithm. An input structures must contain single 
	 * protein chains.
	 * 
	 * @param targets structures containing single protein chains
	 * @param alignmentAlgorithm name of the algorithm
	 * @return dataset with alignment metrics
	 */
	public static Dataset<Row> getQueryVsAllAlignments(
			JavaPairRDD<String, StructureDataInterface> queries, 
			JavaPairRDD<String, StructureDataInterface> targets,
			String alignmentAlgorithm) {

		SparkSession session = SparkSession.builder().getOrCreate();
		@SuppressWarnings("resource") // spark context should not be closed here
		JavaSparkContext sc = new JavaSparkContext(session.sparkContext());

		List<Tuple2<String, Point3d[]>> chains = new ArrayList<>();

		// create a list of chainName/ C Alpha coordinates for query chains
		chains.addAll(queries.mapValues(
				s -> new ColumnarStructureX(s,true).getcAlphaCoordinates()).collect());

		int querySize = chains.size();

		// create a list of chainName/ C Alpha coordinates for target chains
		chains.addAll(targets.mapValues(
				s -> new ColumnarStructureX(s,true).getcAlphaCoordinates()).collect());

		// create an RDD with indices for all query - target pairs (q, t)
		List<Tuple2<Integer, Integer>> pairList = new ArrayList<>(chains.size());
		for (int q = 0; q < querySize; q++) {
			for (int t = querySize; t < chains.size(); t++) {
				pairList.add(new Tuple2<Integer, Integer>(q, t));
			}
		}
		JavaRDD<Tuple2<Integer, Integer>> pairs = sc.parallelize(pairList, NUM_TASKS*sc.defaultParallelism());
		
		// calculate structural alignments for all pairs.
		// the chains are broadcast (copied) to all worker nodes for efficient processing
		JavaRDD<Row> rows = pairs.flatMap(new StructuralAlignmentMapper(sc.broadcast(chains), alignmentAlgorithm));

		// convert rows to a dataset
		return session.createDataFrame(rows, getSchema());
	}

	/**
	 * Creates the schema for the alignment dataset.
	 * @return Schema for the alignment dataset
	 */
	private static StructType getSchema() {
		boolean nullable = false;
		StructField[] sf = {
				DataTypes.createStructField("id", DataTypes.StringType, nullable),
				DataTypes.createStructField("length", DataTypes.IntegerType, nullable),
				DataTypes.createStructField("coverage1", DataTypes.IntegerType, nullable),
				DataTypes.createStructField("coverage2", DataTypes.IntegerType, nullable),
				DataTypes.createStructField("rmsd", DataTypes.FloatType, nullable),
				DataTypes.createStructField("tm", DataTypes.FloatType, nullable)
		};

		return DataTypes.createStructType(sf);
	}

	/**
	 * Creates an RDD of all n*(n-1)/2 unique pairs for pairwise structural alignments.
	 * @param sc spark context
	 * @param n number of protein chains
	 * @return
	 */
	private static JavaRDD<Tuple2<Integer, Integer>> getPairs(JavaSparkContext sc, int n) {
		// create a list of integers from 0 - n-1
		List<Integer> range = IntStream.range(0, n).boxed().collect(Collectors.toList());

		JavaRDD<Integer> pRange = sc.parallelize(range, NUM_TASKS*sc.defaultParallelism());

		// flatmap this list of integers into all unique pairs 
		// (0,1),(0,2),...(0,n-1),  (1,2)(1,3),..,(1,n-1),  (2,3),(2,4),...
		return pRange.flatMap(new FlatMapFunction<Integer, Tuple2<Integer,Integer>>() {
			private static final long serialVersionUID = -432662341173300339L;

			@Override
			public Iterator<Tuple2<Integer, Integer>> call(Integer t) throws Exception {
				List<Tuple2<Integer, Integer>> pairs = new ArrayList<>();

				for (int i = 0; i < t; i++) {
					pairs.add(new Tuple2<Integer, Integer>(i, t));
				}
				return pairs.iterator();
			}
			// The partitions generated here are not well balanced, which would lead to an
			// unbalanced workload. Here we repartition the pairs for efficient processing.
		}).repartition(NUM_TASKS*sc.defaultParallelism()); 
	}
}
