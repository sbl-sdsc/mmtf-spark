package edu.sdsc.mmtf.spark.webfilters;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.spark.api.java.function.Function;
import org.biojava.nbio.structure.symmetry.utils.BlastClustReader;
import org.rcsb.mmtf.api.StructureDataInterface;

import scala.Tuple2;

/**
 * This filter passes through representative structures from the RCSB PDB
 * BlastCLust cluster. A sequence identity thresholds needs to be specified. The
 * representative for each cluster is the first chain in a cluster.
 * 
 * <p>
 * See <a href="https://www.rcsb.org/pdb/statistics/clusterStatistics.do">
 * BlastClust cluster. field names.</a>
 * 
 * <p>
 * Example: find representative PDB entries at 90% sequence identity.
 * 
 * <pre>
 * <code>
 *      int sequenceIdentity = 90;
 *      pdb = pdb.filter(new BlastCluster(90));
 * </code>
 * </pre>
 * 
 * @author Peter Rose
 * @since 0.1.0
 *
 */
public class BlastClusters implements Function<Tuple2<String, StructureDataInterface>, Boolean> {
    private static final long serialVersionUID = -4794067375376198086L;
    private static Set<Integer> clusterLevels = new HashSet<>(Arrays.asList(100, 95, 90, 70, 50, 40, 30));
    private Set<String> pdbIds;

    /**
     * Filters representative PDB structures
     * 
     * @param Sequence
     *            identity (100, 95, 90, 70, 50, 40, 30)
     * @throws IOException
     */
    public BlastClusters(int sequenceIdentity) throws IOException {
        if (!clusterLevels.contains(sequenceIdentity)) {
            throw new IllegalArgumentException("ERROR: invalid sequence identity: " + sequenceIdentity
                    + ". Choose one of the following values: " + clusterLevels);
        }

        pdbIds = new HashSet<>();
        BlastClustReader reader = new BlastClustReader(sequenceIdentity);
        for (List<String> cluster : reader.getPdbChainIdClusters()) {
            pdbIds.add(cluster.get(0)); // add PDB ID.ChainId
            pdbIds.add(cluster.get(0).substring(0, 4)); // add PDB ID
        }
    }

    @Override
    public Boolean call(Tuple2<String, StructureDataInterface> t) {
        return pdbIds.contains(t._1);
    }
}
