package edu.sdsc.mmtf.spark.datasets.demos;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import edu.sdsc.mmtf.spark.datasets.PdbToUniProt;

/**
 * This demo shows how to create datasets of PDB to UniProt
 * chain-level and residue-level mappings.
 * 
 * @author Peter Rose
 * @since 0.2.0
 *
 */
public class PdbToUniProtMappingDemo {

    public static void main(String[] args) throws IOException {
        SparkSession spark = SparkSession.builder().master("local[*]")
                .appName(PdbToUniProtMappingDemo.class.getSimpleName())
                .getOrCreate();
      
        // input can be a list of PDB Ids or a list of PDB Id.ChainIds (e.g., 1STP.A, 4HHB.A)
        List<String> ids = Arrays.asList("1STP","4HHB");
        
        // get dataset of chain-level mappings
        System.out.println("Dataset of chain-level mappings");
        Dataset<Row> chainMappings = PdbToUniProt.getChainMappings(ids);
        chainMappings.show();
        
        // get a subset by filtering the dataset
        System.out.println("Dataset with UniProtId P22629");
        Dataset<Row> subset = PdbToUniProt.getChainMappings().filter("uniprotId = 'P22629'");
        subset.show();
        
        // get dataset of residue-level mappings
        System.out.println("Dataset of residue-level mappings");
        Dataset<Row> residueMappings = PdbToUniProt.getResidueMappings(ids).cache();
        residueMappings.show();
        
        // find PDB Chains that have full structural coverage for the PDB sequence
        Dataset<Row> mappings = PdbToUniProt.getCachedResidueMappings().cache();
        long all = mappings.select("structureChainId").distinct().count();
        long truncated = mappings.filter("pdbResNum IS NULL").select("structureChainId").distinct().count();
        long nonTruncated = all - truncated;
        
        System.out.println("Chains with UniProt mappings: " + all);
        System.out.println("Chains with full structural coverage: " + nonTruncated);
        System.out.println("Chains with partical structural coverage: " + truncated);
        
        spark.stop();
    }

}
