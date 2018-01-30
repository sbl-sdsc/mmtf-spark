package edu.sdsc.mmtf.spark.filters;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.spark.api.java.function.Function;
import org.rcsb.mmtf.api.StructureDataInterface;

import scala.Tuple2;

/**
 * This filter returns entries that contain at least one of the specified groups
 * (residues). Groups are specified by their one, two, or three-letter codes,
 * e.g. "F", "MG", "ATP", as defined in the
 * <a href="https://www.wwpdb.org/data/ccd">wwPDB Chemical Component
 * Dictionary</a>.
 * 
 * @author Peter Rose
 * @since 0.1.0
 *
 */
public class ContainsGroup implements Function<Tuple2<String, StructureDataInterface>, Boolean> {
    private static final long serialVersionUID = -2195111374872792219L;
    private Set<String> groupQuery = null;

    /**
     * This constructor accepts a comma separated list of group names, e.g.,
     * "ATP","ADP"
     * 
     * @param groups
     *            list of group names
     */
    public ContainsGroup(String... groups) {
        this.groupQuery = new HashSet<>(Arrays.asList(groups));
    }

    @Override
    public Boolean call(Tuple2<String, StructureDataInterface> t) throws Exception {
        StructureDataInterface structure = t._2;

        // find number of unique groups
        int uniqueGroups = 0;
        for (int index : structure.getGroupTypeIndices()) {
            uniqueGroups = Math.max(uniqueGroups, index);
        }

        // need to add 1 since the group indices array is zero-based
        uniqueGroups++;

        // add all groups to the set
        Set<String> groupNames = new HashSet<String>(uniqueGroups);
        for (int i = 0; i < uniqueGroups; i++) {
            groupNames.add(structure.getGroupName(i));
        }

        // check if any of the groups are present in the structure
        groupNames.retainAll(groupQuery);
        return !groupNames.isEmpty();
    }
}
