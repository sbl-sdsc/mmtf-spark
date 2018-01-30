package edu.sdsc.mmtf.spark.filters;

import org.apache.spark.api.java.function.Function;
import org.rcsb.mmtf.api.StructureDataInterface;

import scala.Tuple2;

/**
 * This filter return true if this structure contains an alternative location.
 * 
 * @author Peter Rose
 * @since 0.1.0
 *
 */
public class ContainsAlternativeLocations implements Function<Tuple2<String, StructureDataInterface>, Boolean> {
    private static final long serialVersionUID = -8816032515199437989L;

    @Override
    public Boolean call(Tuple2<String, StructureDataInterface> t) throws Exception {
        StructureDataInterface structure = t._2;

        for (char c: structure.getAltLocIds()) {
            if ( c != '\0') return true;
        }

        return false;
    }
}
