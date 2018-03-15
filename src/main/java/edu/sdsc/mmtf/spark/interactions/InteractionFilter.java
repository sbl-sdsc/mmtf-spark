package edu.sdsc.mmtf.spark.interactions;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * A filter to specify criteria for molecular interactions between a query 
 * and a target within a macromolecular structure. The filter specifies criteria 
 * for the query (e.g., a metal ion) and the target (e.g., amino acid residues).
 * Interaction criteria, such as distance cutoff limit the nature of interactions to
 * be considered.
 * 
 * @author Peter Rose
 * @since 0.2.0
 *
 */
public class InteractionFilter implements Serializable {
    private static final long serialVersionUID = 6886734398461616206L;
    
    // interaction criteria
    private double distanceCutoff = Float.MAX_VALUE-1;
    private double normalizedbFactorCutoff = Float.MAX_VALUE-1;
    private int minInteractions = 1;
    private int maxInteractions = 10;

    // query criteria
    private Set<String> queryGroups;
    private boolean includeQueryGroups;
    private Set<String> queryElements;
    private boolean includeQueryElements;
 
    // target criteria
    private Set<String> targetGroups;
    private boolean includeTargetGroups;
    private Set<String> targetElements;
    private boolean includeTargetElements;
    private Set<String> prohibitedTargetGroups;

    /**
     * Gets the maximum interaction distance. At least one pair of query and
     * target atoms must be within this distance cutoff.
     * 
     * @return maximum interaction distance
     */
    public double getDistanceCutoff() {
        return distanceCutoff;
    }

    /**
     * Set the distance cutoff for interacting atoms.
     * 
     * @param distanceCutoff
     *            the maximum interaction distance
     */
    public void setDistanceCutoff(double distanceCutoff) {
        this.distanceCutoff = distanceCutoff;
    }

    /**
     * Returns the square of the maximum interaction distance
     * 
     * @return the square of the maximum interaction distance
     */
    public double getDistanceCutoffSquared() {
        return distanceCutoff * distanceCutoff;
    }

    /**
     * Gets the maximum normalized b-factor (z-score) cutoff for an atom and its
     * interacting neighbor atoms.
     * 
     * @return maximum normalized b-factor cutoff
     */
    public double getNormalizedbFactorCutoff() {
        return normalizedbFactorCutoff;
    }

    /**
     * Sets the maximum normalized b-factor cutoff. This value represents a
     * <a href="https://en.wikipedia.org/wiki/Standard_score">z-score</a>,
     * the signed number of standard deviations by which the b-factor
     * of an atom is above the mean b-factor. High z-scores indicate 
     * either high flexibility and/or experimental error for the atoms
     * involved in the interactions. By setting a cutoff value, not well
     * defined interactions can be excluded.
     * 
     * <pre>
     * Frequently used z-scores:
     * 
     * Confidence level   Tail area   z-score 
     *              90%       0.05    +-1.645 
     *              95%       0.025   +-1.96 
     *              99%       0.005   +-2.576
     * </pre>
     *
     * For example, to include all interactions within the 90% confidence
     * interval, set the normalized b-factor to +1.645.
     * 
     * @param normalizedbFactorCutoff maximum normalized b-factor
     */
    public void setNormalizedbFactorCutoff(double normalizedbFactorCutoff) {
        this.normalizedbFactorCutoff = normalizedbFactorCutoff;
    }

    /**
     * Returns the minimum number of interactions per atom. Atoms that interact
     * with fewer atoms will be discarded.
     * 
     * @return minimum number of interactions per atom
     */
    public int getMinInteractions() {
        return minInteractions;
    }

    /**
     * Sets the minimum number of interactions per atom. Atoms that interact
     * with fewer atoms will be discarded.
     *
     * @param minInteractions minimum number of interactions per atom
     */
    public void setMinInteractions(int minInteractions) {
        this.minInteractions = minInteractions;
    }

    /**
     * Returns the maximum number of interactions per atom. Atoms that interact
     * with more atoms will be discarded.
     * 
     * @return maximum number of interactions per atom
     */
    public int getMaxInteractions() {
        return maxInteractions;
    }

    /**
     * Set the maximum number of required interactions per atom. Atoms that
     * interact with more atoms will be discarded.
     * 
     * @param maxInteractions maximum number of interactions per atom
     */
    public void setMaxInteractions(int maxInteractions) {
        this.maxInteractions = maxInteractions;
    }

    /**
     * Sets the elements to either be included or excluded in the query. Element
     * strings are case sensitive (e.g., "Zn" for Zinc).
     * 
     * <p>
     * Example: Only use elements O, N, S in the query groups to find polar
     * interactions.
     * 
     * <pre>{@code
     * InteractionFilter filter = new InteractionFilter();
     * filter.setQueryElements(true, "O", "N", "S");
     * }</pre>
     * <p>
     * Example: Exclude non-polar elements and hydrogen in query groups and use
     * all other elements to find interactions. This example uses an array
     * instead of a variable argument list.
     * 
     * <pre>{@code
     *     String[] elements = { "C", "H", "P" };
     *     filter.setQueryElements(false, groups);
     * }</pre>
     * 
     * @param include
     *            if true, uses the specified elements in the query, if false,
     *            ignores the specified elements and uses all other elements
     * @param elements
     *            to be included or excluded in query
     */
    public void setQueryElements(boolean include, String... elements) {
        if (this.queryElements != null) {
            throw new IllegalArgumentException("ERROR: QueryElements have already been set.");
        }
        this.includeQueryElements = include;
        this.queryElements = new HashSet<>(elements.length);
        Collections.addAll(this.queryElements, elements);
    }

    /**
     * Sets the elements to either be included or excluded in the target.
     * Element strings are case sensitive (e.g., "Zn" for Zinc).
     * 
     * <p>
     * Example: Use elements O, N, S in the target groups to find polar
     * interactions.
     * 
     * <pre>{@code
     * InteractionFilter filter = new InteractionFilter();
     * filter.setTargetElements(true, "O", "N", "S");
     * }</pre>
     * <p>
     * Example: Exclude non-polar elements and hydrogen in target groups and use
     * all other elements to find interactions.
     * 
     * <pre>{@code
     * filter.setTargetElements(false, "C", "H", "P");
     * }</pre>
     * 
     * @param include
     *            if true, uses the specified elements in the target, if false,
     *            ignores the specified elements and uses all other elements
     * @param elements
     *            to be included or excluded in the target
     */
    public void setTargetElements(boolean include, String... elements) {
        if (this.targetElements != null) {
            throw new IllegalArgumentException("ERROR: TargetElements have already been set.");
        }
        this.includeTargetElements = include;
        this.targetElements = new HashSet<>(elements.length);
        Collections.addAll(this.targetElements, elements);
    }

    /**
     * Sets groups to either be included or excluded in the query. Group names
     * must be upper case (e.g., "ZN" for Zinc).
     * 
     * <p>
     * Example: Find interactions with ATP and ADP.
     * 
     * <pre>{@code
     *     InteractionFilter filter = new InteractionFilter();
     *     filter.setQueryGroups(true, "ATP", "ADP");
     * }</pre>
     * <p>
     * Example: Exclude water and heavy water and use all other groups to find
     * interactions. This example uses an array of groups instead of using a
     * variable argument list.
     * 
     * <pre>{@code
     * String[] groups = { "HOH", "DOD" };
     * filter.setQueryGroups(false, groups);
     * }</pre>
     * 
     * @param include
     *            if true, uses the specified groups in the query, if false,
     *            ignores the specified groups and uses all other groups
     * @param groups
     *            to be included or excluded in query
     */
    public void setQueryGroups(boolean include, String... groups) {
        if (this.queryGroups != null) {
            throw new IllegalArgumentException("ERROR: QueryGroups have already been set.");
        }
        this.includeQueryGroups = include;
        this.queryGroups = new HashSet<>(groups.length);
        Collections.addAll(this.queryGroups, groups);
    }

    /**
     * Sets groups to either be included or excluded in the target. Group names
     * must to upper case (e.g., "ZN" for Zinc).
     * 
     * <p>
     * Example: Find interaction with specific amino acid groups.
     * 
     * <pre>{@code
     * InteractionFilter filter = new InteractionFilter();
     * filter.setTargetGroups(true, "CYS", ,"HIS", "ASP", "GLU");
     * }</pre>
     * <p>
     * Example: Exclude water and heavy water in the target group, but consider
     * all other groups.
     * 
     * <pre>{@code
     * filter.setTargetGroups(false, "HOH", "DOD");
     * }</pre>
     * 
     * @param include
     *            if true, uses set of groups in the target, if false, ignores
     *            the specified groups and uses all other groups
     * @param groups
     *            to be included or excluded in the target
     */
    public void setTargetGroups(boolean include, String... groups) {
        if (this.targetGroups != null) {
            throw new IllegalArgumentException("ERROR: TargetGroups have already been set.");
        }
        this.includeTargetGroups = include;
        this.targetGroups = new HashSet<>(groups.length);
        Collections.addAll(this.targetGroups, groups);
    }

    /**
     * Sets groups to either be included or excluded in the target. Group names
     * must to upper case (e.g., "ZN" for Zinc).
     * 
     * <p>
     * Example: Find interaction with specific amino acid groups.
     * 
     * <pre>{@code
     * InteractionFilter filter = new InteractionFilter();
     * filter.setTargetGroups(true, "CYS", ,"HIS", "ASP", "GLU");
     * }</pre>
     * <p>
     * Example: Exclude water and heavy water in the target group, but consider
     * all other groups.
     * 
     * <pre>{@code
     * filter.setTargetGroups(false, "HOH", "DOD");
     * }</pre>
     * 
     * @param include
     *            if true, uses set of groups in the target, if false, ignores
     *            the specified groups and uses all other groups
     * @param groups
     *            to be included or excluded in the target
     */
    public void setTargetGroups(boolean include, Set<String> groups) {
        if (this.targetGroups != null) {
            throw new IllegalArgumentException("ERROR: TargetGroups have already been set.");
        }
        this.includeTargetGroups = include;
        this.targetGroups = groups;
    }

    /**
     * Sets groups that must not appear in interactions. Any interactions that
     * involves the specified groups will be excluded from the results.
     *
     * <p>
     * Example: Find Zinc interactions, but discard any interactions where the
     * metal is involved in an interaction with water.
     * 
     * <pre>{@code
     *     InteractionFilter filter = new InteractionFilter();
     *     filter.setQueryGroups(true, "ZN");
     *     filter.setProhibitedTargetGroup("HOH");
     * }</pre>
     *
     * @param groups
     *            One or more group names
     */
    public void setProhibitedTargetGroups(String... groups) {
        prohibitedTargetGroups = new HashSet<>(groups.length);
        Collections.addAll(prohibitedTargetGroups, groups);
    }

    /**
     * Sets groups that must not appear in interactions. Any interactions that
     * involves the specified groups will be excluded from the results.
     *
     * <p>
     * Example: Find Zinc interactions, but discard any interactions where the
     * metal is involved in an interaction with water.
     * 
     * <pre>{@code
     * InteractionFilter filter = new InteractionFilter();
     * filter.setQueryGroups(true, "ZN");
     * filter.setProhibitedTargetGroup("HOH");
     * }</pre>
     *
     * @see edu.sdsc.mm.dev.interactions.ExcludedLigandSets
     * @param groups
     *            One or more group names
     */
    public void setProhibitedTargetGroups(Set<String> groups) {
        prohibitedTargetGroups = groups;
    }

    /**
     * Returns true if the specified elements matches the query conditions.
     * 
     * @param element
     *            the element to be checked
     * @return returns true if element matches query conditions
     */
    public boolean isQueryElement(String element) {
        if (queryElements == null) {
            return true;
        }
        if (includeQueryElements) {
            return queryElements.contains(element);
        } else {
            return !queryElements.contains(element);
        }
    }

    /**
     * Returns true if the specified elements matches the target conditions.
     * 
     * @param element
     *            the element to be checked
     * @return returns true if element matches target conditions
     */
    public boolean isTargetElement(String element) {
        if (targetElements == null) {
            return true;
        }
        if (includeTargetElements) {
            return targetElements.contains(element);
        } else {
            return !targetElements.contains(element);
        }
    }

    /**
     * Returns true if the specified group matches the query conditions.
     * 
     * @param group
     *            the group to be checked
     * @return returns true if group matches query conditions
     */
    public boolean isQueryGroup(String group) {
        if (queryGroups == null) {
            return true;
        }
        if (includeQueryGroups) {
            return queryGroups.contains(group);
        } else {
            return !queryGroups.contains(group);
        }
    }

    /**
     * Returns true if the specified group matches the target conditions.
     * 
     * @param group
     *            the group to be checked
     * @return returns true if group matches target conditions
     */
    public boolean isTargetGroup(String group) {
        if (targetGroups == null) {
            return true;
        }
        if (includeTargetGroups) {
            return targetGroups.contains(group);
        } else {
            return !targetGroups.contains(group);
        }
    }

    /**
     * Returns true if the specified group must not occur in and interaction.
     * 
     * @param group
     *            group that must not occur in interactions
     * @return
     */
    public boolean isProhibitedTargetGroup(String group) {
        if (prohibitedTargetGroups == null) {
            return false;
        }
        return prohibitedTargetGroups.contains(group);
    }
}
