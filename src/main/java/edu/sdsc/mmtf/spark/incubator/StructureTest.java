package edu.sdsc.mmtf.spark.incubator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;

public class StructureTest implements Writable, Serializable {
	private static final long serialVersionUID = 1L;
	private String structureId;
    private int numModels;
    private float[] xCoords;
    
	public String getStructureId() {
		return structureId;
	}

	public void setStructureId(String structureId) {
		this.structureId = structureId;
	}

	public int getNumModels() {
		return numModels;
	}

	public void setNumModels(int numModels) {
		this.numModels = numModels;
	}

	public float[] getxCoords() {
		return xCoords;
	}

	public void setxCoords(float[] xCoords) {
		this.xCoords = xCoords;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		int len = structureId.length() + 4 + 4 + xCoords.length*4;
        ByteBuffer buf = ByteBuffer.allocateDirect(len);
        buf.putInt(len);
        buf.putInt(structureId.getBytes().length);
        buf.put(structureId.getBytes());
		buf.putInt(numModels);
		buf.putInt(xCoords.length);
		for (float x: xCoords) {
			buf.putFloat(x);
		}	
		out.write(buf.array());
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		int len = in.readInt();
		byte[] data = new byte[len];
		in.readFully(data);
		ByteBuffer b = ByteBuffer.wrap(data);
		int sLen = b.getInt();
		byte[] sBytes = new byte[sLen];
		b.get(sBytes, 0, sLen);
		this.structureId = new String(sBytes);
		this.numModels = b.getInt();
		int l = b.getInt();
		this.xCoords = new float[l];
		for (int i = 0; i < len; i++) {
			xCoords[i] = b.getFloat();
		}	
	}
}
