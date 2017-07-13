package edu.sdsc.mmtf.spark.mappers;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

/**
 * Maps chain sequences to its sequence segments. 
 * 
 * TODO
 * @author Yue Yu
 */
public class StructureToSecondaryStructureSegments implements FlatMapFunction<Row, Row> {
	private static final long serialVersionUID = 8907755561668400639L;

	private int length;
	
	/*
	 * Constructor sets the segment length. The length must
	 * be an odd number
	 */
	public StructureToSecondaryStructureSegments(int length) {
		this.length = length;
	}
	
	@Override
	public Iterator<Row> call(Row t) throws Exception {
		//get information from the input Row
		String structureChainId = t.getString(0);
		String sequence = t.getString(1);
		String dsspQ8 = t.getString(5);
		String dsspQ3 = t.getString(6);
		
		int numSegments = Math.max(0, sequence.length() - length);
		List<Row> sequences = new ArrayList<>(numSegments);
		
		for (int i = 0; i < sequence.length() - length; i++)
		{
			String currSeq = sequence.substring(i, i+length);
			String labelQ8 = dsspQ8.substring(i + length/2,i + length/2 + 1);
			String labelQ3 = dsspQ3.substring(i + length/2,i + length/2 + 1);
			if ( !labelQ8.equals("X") && !labelQ3.equals("X"))
			{
				sequences.add( RowFactory.create(structureChainId, currSeq, labelQ8, labelQ3) );
			}
		}
		return sequences.iterator();
	}
}