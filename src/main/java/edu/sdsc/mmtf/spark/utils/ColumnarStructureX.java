package edu.sdsc.mmtf.spark.utils;

import java.util.ArrayList;
import java.util.List;

import javax.vecmath.Point3d;
import javax.vecmath.Point3f;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.rcsb.mmtf.api.StructureDataInterface;

/**
 * Provides efficient access to derived structure information in the form of 
 * atom-based arrays. Data are lazily initialized as needed.
 * 
 * @author Peter Rose
 * @since 0.2.0
 *
 */
public class ColumnarStructureX extends ColumnarStructure {
    private static final long serialVersionUID = -7149627754721953351L;

    private float[] normalizedbFactors;
    private float[] clampedNormalizedbFactors;

    public ColumnarStructureX(StructureDataInterface structure, boolean firstModelOnly) {
        super(structure, firstModelOnly);
    }

    /**
     * Returns z-scores for B-factors (normalized B-factors).
     * 
     * Critical z-score values: Confidence level Tail Area z critical 
     *                          90%              0.05      +-1.645 
     *                          95%              0.025     +-1.96 
     *                          99%              0.005     +-2.576
     * 
     * @return
     */
    public float[] getNormalizedbFactors() {
        if (normalizedbFactors == null) {
            normalizedbFactors = new float[getNumAtoms()];

            float[] bFactors = getbFactors();
            String[] types = getEntityTypes();

            // TODO need to handle B-factors for experimental methods
            // where they are undefined
            DescriptiveStatistics stats = new DescriptiveStatistics();
            for (int i = 0; i < getNumAtoms(); i++) {
                if (! (types[i].equals("WAT"))) {
                    stats.addValue(bFactors[i]);
                }
            }
            double mean = stats.getMean();
            double stddev = stats.getStandardDeviation();

            for (int i = 0; i < getNumAtoms(); i++) {
                normalizedbFactors[i] = (float) ((bFactors[i] - mean) / stddev);
            }
        }

        return normalizedbFactors;
    }

    /**
     * Returns normalized B-factors that are clamped to the [-1, 1] interval
     * using the method of Liu et al. B-factors are normalized and scaled the
     * 90% confidence interval of the B-factor to [-1, 1]. Any value outside of
     * the 90% confidence interval is set to either -1 or 1, whichever is
     * closer.
     *
     * <p>
     * Reference: Liu et al. BMC Bioinformatics 2014, 15(Suppl 16):S3, Use
     * B-factor related features for accurate classification between protein
     * binding interfaces and crystal packing contacts See <a href=
     * "https://doi.org/10.1186/1471-2105-15-S16-S3">doi:10.1186/1471-2105-15-S16-S3</a>.
     * 
     * @return clamped normalized B-factors
     */
    public float[] getClampedNormalizedbFactors() {
        if (clampedNormalizedbFactors == null) {
            clampedNormalizedbFactors = new float[getNumAtoms()];

            float[] normalizedbFactors = getNormalizedbFactors();

            for (int i = 0; i < getNumAtoms(); i++) {
                // normalize and scale the 90% confidence interval of the B
                // factor
                // to [-1, 1].
                double s = normalizedbFactors[i] / 1.645;
                // set any value outside the 90% confidence interval to either
                // -1 or
                // 1, whichever is closer.
                clampedNormalizedbFactors[i] = (float) Math.min(Math.max(s, -1.0), 1.0);
            }
        }

        return clampedNormalizedbFactors;
    }

    public Point3f[] getcAlphaCoordinatesF() {
        List<Integer> indices = getCalphaAtomIndices();

        float[] x = getxCoords();
        float[] y = getyCoords();
        float[] z = getzCoords();

        Point3f[] calpha = new Point3f[indices.size()];
        for (int i = 0; i < calpha.length; i++) {
            int index = indices.get(i);
            calpha[i] = new Point3f(x[index], y[index], z[index]);
        }
        return calpha;
    }

    public Point3d[] getcAlphaCoordinates() {
        List<Integer> indices = getCalphaAtomIndices();

        float[] x = getxCoords();
        float[] y = getyCoords();
        float[] z = getzCoords();

        Point3d[] calpha = new Point3d[indices.size()];
        for (int i = 0; i < calpha.length; i++) {
            int index = indices.get(i);
            calpha[i] = new Point3d(x[index], y[index], z[index]);
        }
        return calpha;
    }

    public List<Integer> getCalphaAtomIndices() {
        String[] entityTypes = getEntityTypes();
        String[] atomNames = getAtomNames();

        List<Integer> caIndices = new ArrayList<>(getNumAtoms());
        for (int i = 0; i < getNumAtoms(); i++) {
            if (entityTypes[i].equals("PRO") && atomNames[i].equals("CA")) {
                caIndices.add(i);
            }
        }
        return caIndices;
    }
}
