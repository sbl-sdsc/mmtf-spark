/**
 * 
 */
package edu.sdsc.mmtf.spark.io;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.rcsb.mmtf.decoder.ReaderUtils;

import scala.Tuple2;

/**
 * @author peter
 *
 */
public class MmtfSequenceFileConverter {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

	    if (args.length != 2) {
	        System.err.println("Usage: SequenceFileDecompressor <compressed hadoop sequence file> <uncompressed hadoop sequence file>");
	        System.exit(1);
	    }
	    
	    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(MmtfSequenceFileConverter.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
		   
		sc.sequenceFile(args[0], Text.class, BytesWritable.class)
		.mapToPair(t -> new Tuple2<Text, BytesWritable>(new Text(t._1), new BytesWritable(ReaderUtils.deflateGzip(t._2.copyBytes()))))
//		.saveAsHadoopFile(args[1], Text.class, BytesWritable.class, SequenceFileOutputFormat.class, GzipCodec.class);
		.saveAsHadoopFile(args[1], Text.class, BytesWritable.class, SequenceFileOutputFormat.class, BZip2Codec.class);
	    
	    sc.close();
	}

}
