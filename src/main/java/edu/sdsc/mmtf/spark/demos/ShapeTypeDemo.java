package edu.sdsc.mmtf.spark.demos;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.biojava.nbio.structure.Atom;
import org.biojava.nbio.structure.Structure;
import org.biojava.nbio.structure.StructureTools;
import org.biojava.nbio.structure.geometry.MomentsOfInertia;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.filters.ContainsLProteinChain;
import edu.sdsc.mmtf.spark.filters.PolymerComposition;
import edu.sdsc.mmtf.spark.io.MmtfReader;
import edu.sdsc.mmtf.spark.mappers.StructureToBioJava;
import edu.sdsc.mmtf.spark.mappers.StructureToPolymerChains;
import edu.sdsc.mmtf.spark.mappers.StructureToPolymerSequences;
import edu.sdsc.mmtf.spark.ml.JavaRDDToDataset;
import edu.sdsc.mmtf.spark.ml.SequenceWord2Vector;
import edu.sdsc.mmtf.spark.rcsbfilters.BlastClusters;
import edu.sdsc.mmtf.spark.utils.PolymerSequenceExtractor;
import scala.Tuple2;

public class ShapeTypeDemo {

	public static void main(String[] args) throws IOException {

		if (args.length != 2) {
			System.err.println("Usage: " + ShapeTypeDemo.class.getSimpleName() + " <hadoop sequence file> <dataset output file");
			System.exit(1);
		}
		
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(Demo1b.class.getSimpleName());
		JavaSparkContext sc = new JavaSparkContext(conf);

		long start = System.nanoTime();

		// load a representative PDB chain from the 40% seq. identity Blast Clusters
		int sequenceIdentity = 40;
		JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader
				.readSequenceFile(args[0], sc)
				.filter(new BlastClusters(sequenceIdentity)) // filter by pdb id using a non-redundant "BlastClust" subset
				.flatMapToPair(new StructureToPolymerChains()) // extract polymer chains
				.filter(new BlastClusters(sequenceIdentity)) // this filters is more selective by using chain ids
				.filter(new ContainsLProteinChain()) // filter out for example D-proteins
				.filter(new PolymerComposition(PolymerComposition.AMINO_ACIDS_20));
		
		// get a data set with sequence info
		Dataset<Row> seqData = PolymerSequenceExtractor.getDataset(pdb);
		
		// convert to BioJava data structure
		JavaPairRDD<String, Structure> structures = pdb.mapToPair(new StructureToBioJava());

		// calculate shape data and convert to dataset
		JavaRDD<Row> rows = structures.map(t -> getShapeData(t));
		Dataset<Row> data = JavaRDDToDataset.getDataset(rows, "structureChainId", "shape");
		// there are only few symmetric chain, leave them out
	    data = data.filter("shape != 'SYMMETRIC'");

	    // join calculated data with the sequence data
		data = seqData.join(data, "structureChainId");
	    data.show(10);

		// create a Word2Vector representation of the protein sequences
		int n = 2; // create 2-grams
		int windowSize = 25; // 25-amino residue window size for Word2Vector
		int vectorSize = 50; // dimension of feature vector	
		data = SequenceWord2Vector.addFeatureVector(seqData, n, windowSize, vectorSize).cache();

		// save data in .parquet file
	    data.write().mode("overwrite").format("parquet").save(args[1]);
		
	    long end = System.nanoTime();
		System.out.println((end-start)/1E9 + " sec.");
		
		sc.close();
	}
	
	private static Row getShapeData(Tuple2<String, Structure> t) {
		String key = t._1;
		Structure structure = t._2;

		return RowFactory.create(
				key, // primary key for this dataset
				// this seq. has lots of XXXX, seems to be an error 
				// in populating the BioJava data structure from mmtf
	//			structure.getChainByIndex(0).getSeqResSequence(), 
				calcShape(structure)
				);
	}
	
	private static String calcShape(Structure structure) {
		// calculate moments of inertia for C-alpha atoms
		MomentsOfInertia moi = new MomentsOfInertia();
		for (Atom a: StructureTools.getAtomCAArray(structure)) {
			moi.addPoint(a.getCoordsAsPoint3d(), 1.0);
		}
		
		// calculate symmetry based on the moments of inertia
		return moi.getSymmetryClass(0.05).toString();
	} 
}
