package edu.sdsc.mmtf.spark.mappers;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

/**
 * Maps chain sequences to its sequence segments. 
 * TODO
 * @author Yue Yu
 */
public class SequenceToSegment implements FlatMapFunction<Row, Row> {
	private static final long serialVersionUID = 8907755561668400639L;

	private int length;
	
	/*
	 * TODO
	 */
	public SequenceToSegment() {
		this.length = 25;
	}
	
	/*
	 * TODO
	 * If the argument is set, it will be used as the value of segment length.
	 */
	public SequenceToSegment(int length) {
		this.length = length;
	}
	
	@Override
	public Iterator<Row> call(Row t) throws Exception {
		//get information from the input Row
		String structureChainId = (String) t.get(0);
		String sequence = (String) t.get(1);
		String dsspQ8 = (String) t.get(5);
		String dsspQ3 = (String) t.get(6);
		
		String currSeq = "";		
		String labelQ8;
		String labelQ3;
		List<Row> sequences = new ArrayList<>();
		
		for(int i = 0; i < sequence.length() - length; i++)
		{
			currSeq = sequence.substring(i, i+length);
			labelQ8 = dsspQ8.substring(i + length/2,i + length/2 + 1);
			labelQ3 = dsspQ3.substring(i + length/2,i + length/2 + 1);
			if( !labelQ8.equals("X") && !labelQ3.equals("X"))
			{
				sequences.add( RowFactory.create(structureChainId, currSeq, labelQ8, labelQ3) );
			}
		}
		return sequences.iterator();
	}

}