package edu.sdsc.mmtf.spark.datasets.demos;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import edu.sdsc.mmtf.spark.datasets.G2SDataset;
import edu.sdsc.mmtf.spark.datasets.MyVariantDataset;

/**
 * This demo shows how to query for missense mutations and map them to 
 * residue positions on 3D protein structures in the PDB. Missence mutations
 * are queried using MyVariant.info web services (see {@link MyVariantDataset}) 
 * and genetic location to PDB mappings are calculated using G2S web services 
 * (see {@link G2SDataset}).
 * 
 * @author Peter Rose
 * @since 0.2.0
 * 
 */
public class MutationToStructureDemo {

    public static void main(String[] args) throws IOException {
        SparkSession spark = SparkSession.builder().master("local[*]").appName(MutationToStructureDemo.class.getSimpleName())
                .getOrCreate();

        // find missense mutations that map to UniProt ID P15056 (BRAF)
        // that are annotated as pathogenic or likely pathogenic in ClinVar.
        List<String> uniprotIds = Arrays.asList("P15056"); // BRAF: P15056
        String query = "clinvar.rcv.clinical_significance:pathogenic OR clinvar.rcv.clinical_significance:likely pathogenic";
        Dataset<Row> df = MyVariantDataset.getVariations(uniprotIds, query).cache();
        System.out.println("BRAF missense mutations: " + df.count());
        df.show();
        
        // extract the list of variant Ids
        List<String> variantIds = df.select("variationId").as(Encoders.STRING()).collectAsList();
        
        // map to PDB structures
        Dataset<Row> ds = G2SDataset.getPositionDataset(variantIds);
        ds = ds.sort("structureId","chainId","pdbPosition");
        ds.show();

        spark.close(); 
    }
}