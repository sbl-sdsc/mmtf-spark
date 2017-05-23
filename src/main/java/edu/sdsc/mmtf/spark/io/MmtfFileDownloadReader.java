package edu.sdsc.mmtf.spark.io;

import java.io.IOException;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.rcsb.mmtf.api.StructureDataInterface;
import org.rcsb.mmtf.decoder.GenericDecoder;
import org.rcsb.mmtf.decoder.ReaderUtils;

import scala.Tuple2;

public class MmtfFileDownloadReader {

	public static JavaPairRDD<String, StructureDataInterface> read(List<String> pdbIds, JavaSparkContext sc) {
		return sc
				.parallelize(pdbIds)
				.mapToPair(t -> new Tuple2<String, StructureDataInterface>(t, getStructure(t)));	
	}
	
	private static StructureDataInterface getStructure(String pdbId) throws IOException {
		return new GenericDecoder(ReaderUtils.getDataFromUrl(pdbId));
	}
}
