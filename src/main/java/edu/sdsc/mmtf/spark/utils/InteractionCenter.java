package edu.sdsc.mmtf.spark.utils;

import java.io.Serializable;

import javax.vecmath.Point3d;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;


/**
 * InteractionCenter stores information about an atom involved in a molecular
 * interaction.
 * 
 * @author Peter Rose
 * @since 0.2.0
 *
 */
public class InteractionCenter implements Serializable {
    private static final long serialVersionUID = 3898493926125720831L;

    public static final int LENGTH = 7; // number of data fields

    private String atomName;
    private String element;
    private String groupName;
    private String groupNumber; // group number + insertion code, e.g. 101A
    private String type;
    private String chainName;
    private int sequencePosition;
    private Point3d coordinates;
    private double normalizedbFactor;

    /**
     * Default constructor
     */
    public InteractionCenter() {};
    
    /**
     * Constructor using columnar structure with an atom index.
     * 
     * @param structure
     *            columnar structure
     * @param atomIndex
     *            index to the interacting atom
     */
    public InteractionCenter(ColumnarStructureX structure, int atomIndex) {
        this.setAtomName(structure.getAtomNames()[atomIndex]);
        this.setElement(structure.getElements()[atomIndex]);
        this.setGroupName(structure.getGroupNames()[atomIndex]);
        this.setGroupNumber(structure.getGroupNumbers()[atomIndex]);
        this.setType(structure.getEntityTypes()[atomIndex]);
        this.setChainName(structure.getChainNames()[atomIndex]);
        this.setSequencePosition(structure.getSequencePositions()[atomIndex]);
        this.setCoordinates(new Point3d(structure.getxCoords()[atomIndex], structure.getyCoords()[atomIndex],
                structure.getzCoords()[atomIndex]));
        this.setNormalizedbFactor(structure.getNormalizedbFactors()[atomIndex]);
    }

    /**
     * Gets the atom name.
     * 
     * @return name of the atom
     */
    public String getAtomName() {
        return atomName;
    }

    /**
     * Sets the atom name.
     * 
     * @param atomName
     *            name of the atom
     */
    public void setAtomName(String atomName) {
        this.atomName = atomName;
    }

    /**
     * Gets the case-sensitive element symbol.
     * 
     * @return element symbol
     */
    public String getElement() {
        return element;
    }

    /**
     * Sets the case-sensitive element symbol.
     * 
     * @param element
     *            element symbol
     */
    public void setElement(String element) {
        this.element = element;
    }

    /**
     * Gets the name of the group. This name is the chemical component id of this
     * group.
     * 
     * @return name of group
     */
    public String getGroupName() {
        return groupName;
    }

    /**
     * Sets the name of the group. This name is the chemical component id of
     * this group.
     * 
     * @param groupName
     *            name of group
     */
    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    /**
     * Gets the group number for the interaction center. A group number consists
     * of the residue number (e.g., 101) plus an optional insertion code (e.g.,
     * A): 101A.
     * 
     * @return group number
     */
    public String getGroupNumber() {
        return groupNumber;
    }

    /**
     * Sets the group number for the interaction center. A group number consists
     * of the residue number (e.g., 101) plus an optional insertion code (e.g.,
     * A): 101A.
     * 
     * @param groupNumber
     */
    public void setGroupNumber(String groupNumber) {
        this.groupNumber = groupNumber;
    }

    /**
     * Gets the type of the group.
     * 
     * @return type of group.
     */
    public String getType() {
        return type;
    }

    /**
     * Sets the type of the group.
     * 
     * @param type
     *            the type of the group
     */
    public void setType(String type) {
        this.type = type;
    }

    /**
     * Gets the chainName. This corresponds to the "chain Id" in PDB files.
     * 
     * @return chain name
     */
    public String getChainName() {
        return chainName;
    }

    /**
     * Sets the chainName. This corresponds to the "chain Id" in PDB files.
     * 
     * @param chainName
     *            the name of the chain
     */
    public void setChainName(String chainName) {
        this.chainName = chainName;
    }

    /**
     * Gets an index into the one-letter polymer sequence. 
     * This index is zero-based. If the interaction center is
     * not a polymer atom, this index is -1.
     * 
     * @return index into polymer sequence
     */
    public int getSequencePosition() {
        return sequencePosition;
    }

    /**
     * Sets an index into the one-letter polymer sequence. 
     * This index is zero-based. If the interaction center is
     * not a polymer atom, this index is -1.
     * 
     * @param sequencePosition
     *            index into polymer sequence
     */
    public void setSequencePosition(int sequencePosition) {
        this.sequencePosition = sequencePosition;
    }

    /**
     * Sets the position of the interaction center.
     * 
     * @return the position of the interaction center
     */
    public Point3d getCoordinates() {
        return coordinates;
    }

    /**
     * Gets the position of the interaction center.
     * 
     * @param coordinates
     *            the position of the interaction center
     */
    public void setCoordinates(Point3d coordinates) {
        this.coordinates = coordinates;
    }

    /**
     * Gets the normalized b-factor.
     * 
     * @return
     */
    public double getNormalizedbFactor() {
        return normalizedbFactor;
    }

    /**
     * Sets the normalized b-factor.
     * 
     * @param normalizedbFactor
     */
    public void setNormalizedbFactor(double normalizedbFactor) {
        this.normalizedbFactor = normalizedbFactor;
    }

    /**
     * Returns the number of data items in an interaction center. Note, not all
     * data are currently included.
     * 
     * @return the number of data items in an interaction center
     */
    public static int getLength() {
        return LENGTH;
    }

    /**
     * Returns an array of objects representing this interaction center. Note,
     * not all data are currently included. This method is used to create Spark
     * Datasets.
     * 
     * @return array of objects representing this interaction center
     */
    public Object[] getAsObject() {
        return new Object[] { atomName, element, groupName, groupNumber, type, chainName, (float) normalizedbFactor};
    }

    /**
     * Returns a schema to create Spark Datasets. This schema must match the
     * order in which the data are return by the {@code getAsObject()} method.
     * 
     * @param index
     *            an integer index to label an interaction center
     * @return schema to represent an interaction center in a Spark Dataset.
     */
    public static StructField[] getStructFields(int index) {
        boolean nullable = true;
        return new StructField[] { DataTypes.createStructField("atom" + index, DataTypes.StringType, nullable),
                DataTypes.createStructField("element" + index, DataTypes.StringType, nullable),
                DataTypes.createStructField("group" + index, DataTypes.StringType, nullable),
                DataTypes.createStructField("groupNum" + index, DataTypes.StringType, nullable),
                DataTypes.createStructField("type" + index, DataTypes.StringType, nullable),
                DataTypes.createStructField("chain" + index, DataTypes.StringType, nullable),
                DataTypes.createStructField("nbFactor" + index, DataTypes.FloatType, nullable)};
    }
}
