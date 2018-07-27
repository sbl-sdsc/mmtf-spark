package edu.sdsc.mmtf.spark.datasets;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class AdvancedQueryDatasetTest {
    private SparkSession spark;

    @Before
    public void setUp() throws Exception {
        spark = SparkSession.builder().master("local[*]").appName(AdvancedQueryDatasetTest.class.getSimpleName())
                .getOrCreate();
    }

    @After
    public void tearDown() throws Exception {
        spark.close();
    }

    @Test
    public void test1() throws IOException {
        String query = "<orgPdbQuery>" 
                     + "<queryType>org.pdb.query.simple.StoichiometryQuery</queryType>"
                     + "<stoichiometry>A3B3C3</stoichiometry>" 
                     + "</orgPdbQuery>";

        Dataset<Row> ds = AdvancedSearchDataset.getDataset(query);

        assertEquals(1, ds.filter("structureId = '1A5K'").count());
    }

    @Test
    public void test2() throws IOException {
        String query = "<orgPdbQuery>" 
                     + "<queryType>org.pdb.query.simple.TreeEntityQuery</queryType>"
                     + "<t>1</t>"
                     + "<n>9606</n>" 
                     + "</orgPdbQuery>";

        Dataset<Row> ds = AdvancedSearchDataset.getDataset(query);

        assertEquals(2, ds.filter("structureChainId = '10GS.A' OR structureChainId = '10GS.B'").count());
    }

    @Test
    public void test3() throws IOException {
        String query = "<orgPdbQuery>" 
                     + "<queryType>org.pdb.query.simple.ChemSmilesQuery</queryType>"
                     + "<smiles>CC(C)C1=C(Br)C(=O)C(C)=C(Br)C1=O</smiles>" + "<target>Ligand</target>"
                     + "<searchType>Substructure</searchType>" + "<polymericType>Any</polymericType>" + "</orgPdbQuery>";

        Dataset<Row> ds = AdvancedSearchDataset.getDataset(query);

        assertEquals(1, ds.filter("ligandId = 'BNT'").count());
    }
}
