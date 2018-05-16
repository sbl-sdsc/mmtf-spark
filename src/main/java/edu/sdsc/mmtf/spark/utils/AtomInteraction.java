package edu.sdsc.mmtf.spark.utils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.vecmath.Point3d;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;


/**
 * AtomInteraction contains interaction information of a central atom
 * with its interacting neighbors (coordination sphere). Once this 
 * data structure is filled with interaction centers, this class 
 * calculates various geometric properties such as distance, angles, 
 * and order parameters for the interacting atoms (see {@link CoordinationGeometry}). 
 * Finally, it provides methods for creating row-wise representations of 
 * the data in Spark Datasets.
 * 
 * <p> Two row-wise representations of results are supported:
 * <ul>
 * <li> Rows of interacting atom pairs and distances
 * <li> Row of central atom and all neighbor atoms, distances, angles, and order parameters
 * </ul>
 * 
 * @author Peter Rose
 * @since 0.2.0
 * @see CoordinationGeometry
 *
 */
public class AtomInteraction {
	private String structureId;
	
	// interaction centers
	private InteractionCenter center;
	private List<InteractionCenter> neighbors = new ArrayList<>();
	
	// geometric parameters
	/* trigonal orientational order parameter */
    private Float q3;
    /* tetrahedral orientational order parameter */
    private Float q4;
    /* trigonal bipyramidal orientational order parameter */
    private Float q5;
    /* octahedral orientational order parameter */
    private Float q6;
	private Float[] distances;
	private Float[] angles;

	/**
	 * Returns the structure identifier.
	 * 
	 * @return structure identifier
	 */
	public String getStructureId() {
		return structureId;
	}

	/**
	 * Sets the structure identifier.
	 * 
	 * @param structureId structure identifier
	 */
	public void setStructureId(String structureId) {
		this.structureId = structureId;
	}

	/**
	 * Calculates geometric properties of the coordination sphere.
	 * The geometric properties include orientational order parameters
	 * that describe the arrangement of atoms in the coordination sphere,
	 * distances and angles of the neighbor atoms with the center atom.
	 * 
	 */
	private void calcCoordinationGeometry(int maxInteractions) {
		List<Point3d> neighborPoints = new ArrayList<Point3d>();

		for (InteractionCenter neighbor : neighbors) {
			if (neighbor.getCoordinates() != null) {
				neighborPoints.add(neighbor.getCoordinates());
			}
		}

		Point3d[] points = neighborPoints.toArray(new Point3d[neighborPoints.size()]);

		CoordinationGeometry geom = new CoordinationGeometry(center.getCoordinates(), points);

		// calculate distances to the central atom
		distances = new Float[maxInteractions];
		double[] dist = geom.getDistances();
		for (int i = 0; i < dist.length; i++) {
			distances[i] = (float) dist[i];
		}

		// calculate angles among all interacting atoms with the central atom
		angles = new Float[maxInteractions * (maxInteractions - 1) / 2];
		double[] ang = geom.getAngles();
		for (int i = 0; i < ang.length; i++) {
			angles[i] = (float) ang[i];
		}
		
		// calculate orientational order parameters
		if (points.length > 2) {
			q3 = (float) geom.q3();
		}
		if (points.length > 3) {
			q4 = (float) geom.q4();
		}
		if (points.length > 4) {
			q5 = (float) geom.q5();
		}
		if (points.length > 5) {
			q6 = (float) geom.q6();
		}
	}

	/**
	 * Returns information about the central atom of a coordination sphere.
	 * 
	 * @return central atom information
	 */
	public InteractionCenter getCenter() {
		return center;
	}

	/**
	 * Sets the central atom information of a coordination sphere.
	 * 
	 * @param center
	 */
	public void setCenter(InteractionCenter center) {
		this.center = center;
	}

	/**
	 * Returns the number of neighbor atoms that interact with the central atom.
	 * 
	 * @return number of neighbor atoms that interact with the central atom
	 */
	public int getNumInteractions() {
		return neighbors.size();
	}

	/**
	 * Returns information about the interacting neighbor atoms.
	 * 
	 * @return interaction centers
	 */
	public List<InteractionCenter> getInteractions() {
		return neighbors;
	}

	/**
	 * Adds a neighbor interaction center.
	 * 
	 * @param neighbor an interaction with the central atom
	 */
	public void addNeighbor(InteractionCenter neighbor) {
		this.neighbors.add(neighbor);
	}

	/**
	 * Returns interactions and geometric information in a single row.
	 * 
	 * @return row of interactions and geometric information
	 */
	public Row getMultipleInteractionsAsRow(int maxInteractions) {
		// pad interaction centers and distances with nulls, if necessary,
		// since each row must be of fixed length
		while (getNumInteractions() < maxInteractions) {
			neighbors.add(new InteractionCenter());
		}

		int length = InteractionCenter.getLength();

		Object[] data = new Object[getNumColumns(maxInteractions)];

		int index = 0;
		data[index++] = structureId;
		data[index++] = getNumberOfPolymerChains();
		
		calcCoordinationGeometry(maxInteractions);
		data[index++] = q3;
		data[index++] = q4;
		data[index++] = q5;
		data[index++] = q6;
		

		// copy data for query atom
		System.arraycopy(center.getAsObject(), 0, data, index, length);
		index += length;

		// copy data for interacting atoms
		for (int i = 0; i < neighbors.size(); i++) {
			System.arraycopy(neighbors.get(i).getAsObject(), 0, data, index, length);
			index += length;
			data[index++] = distances[i];
		}

		// copy angles
		System.arraycopy(angles, 0, data, index, angles.length);
		index += length;

		return RowFactory.create(data);
	}

	/**
	 * Returns rows of pairwise interactions with the central atom.
	 * 
	 * @return rows of pairwise interactions with the central atom
	 */
	public List<Row> getPairInteractionsAsRows() {
		List<Row> rows = new ArrayList<>(neighbors.size());

		int length = InteractionCenter.getLength();
		
		calcCoordinationGeometry(neighbors.size());

		// copy data of the interacting atoms
		for (int i = 0; i < neighbors.size(); i++) {
			Object[] data = new Object[2 * length + 2];
			int index = 0;
			data[index++] = structureId;
			System.arraycopy(center.getAsObject(), 0, data, index, length);
			index += length;
			System.arraycopy(neighbors.get(i).getAsObject(), 0, data, index, length);
			index += length;
			data[index++] = distances[i];
			rows.add(RowFactory.create(data));
		}

		return rows;
	}

	/**
	 * Returns the number of unique polymer chains in the coordination sphere.
	 * 
	 * @return number of unique polymer chains in the coordination sphere
	 */
	private int getNumberOfPolymerChains() {
		Set<String> chainNameSet = new HashSet<>();
		for (InteractionCenter center : neighbors) {
			if (center != null && center.getSequencePosition() >= 0) {
				chainNameSet.add(center.getChainName());
			}
		}

		return chainNameSet.size();
	}

	/**
	 * Returns the schema for a row of atom interaction information. The schema
	 * is used to create a Dataset<Row> from the row information.
	 * 
	 * @return schema for dataset
	 */
	public static StructType getSchema(int maxInteractions) {
		int length = InteractionCenter.getLength();
		StructField[] sf = new StructField[getNumColumns(maxInteractions)];

		int index = 0;
		sf[index++] = DataTypes.createStructField("pdbId", DataTypes.StringType, false);
		sf[index++] = DataTypes.createStructField("polyChains", DataTypes.IntegerType, false);
		sf[index++] = DataTypes.createStructField("q3", DataTypes.FloatType, true);
		sf[index++] = DataTypes.createStructField("q4", DataTypes.FloatType, true);
		sf[index++] = DataTypes.createStructField("q5", DataTypes.FloatType, true);
		sf[index++] = DataTypes.createStructField("q6", DataTypes.FloatType, true);

		// copy schema info for query atom
		System.arraycopy(InteractionCenter.getStructFields(0), 0, sf, index, length);
		index += length;

		// copy schema info for interacting atoms and their distances
		for (int i = 0; i < maxInteractions; i++) {
			System.arraycopy(InteractionCenter.getStructFields(i + 1), 0, sf, index, length);
			index += length;
			sf[index++] = DataTypes.createStructField("distance" + (i + 1), DataTypes.FloatType, true);
		}

		// add schema for angles
		for (int i = 0; i < maxInteractions - 1; i++) {
			for (int j = i + 1; j < maxInteractions; j++) {
				sf[index++] = DataTypes.createStructField("angle" + (i + 1) + "-" + (j + 1), DataTypes.FloatType, true);
			}
		}

		return new StructType(sf);
	}

	/**
	 * Returns the schema for a row of pairwise atom interactions. 
	 * The schema is used to create a Dataset<Row> from the row information.
	 * 
	 * @return schema for dataset
	 */
	public static StructType getPairInteractionSchema() {
		int length = InteractionCenter.getLength();
		StructField[] sf = new StructField[2 * length + 2];

		int index = 0;
		sf[index++] = DataTypes.createStructField("pdbId", DataTypes.StringType, false);

		// copy schema info for query atom
		System.arraycopy(InteractionCenter.getStructFields(0), 0, sf, index, length);
		index += length;

		// copy schema info for interacting atoms and their distance
		System.arraycopy(InteractionCenter.getStructFields(1), 0, sf, index, length);
		index += length;
		sf[index++] = DataTypes.createStructField("distance1", DataTypes.FloatType, true);

		return new StructType(sf);
	}

	/**
	 * Returns the number of columns in a Row
	 * 
	 * @return
	 */
	private static int getNumColumns(int maxInteractions) {
		int length = InteractionCenter.getLength();
		// the number of columns:
		// structureId + polymerChains + q3 + q4 + q5 + q6: 6
		// query + interaction centers: (maxInteractions + 1) * length
		// distances: maxInteractions
		// angles: maxInteractions * (maxInteractions - 1) / 2;
		int numAngles = maxInteractions * (maxInteractions - 1) /2;
		return 6 + (maxInteractions + 1) * length + maxInteractions + numAngles;
	}
}
