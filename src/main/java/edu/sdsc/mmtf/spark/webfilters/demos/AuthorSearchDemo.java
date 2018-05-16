package edu.sdsc.mmtf.spark.webfilters.demos;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.io.MmtfReader;
import edu.sdsc.mmtf.spark.webfilters.PdbjMineSearch;

/**
 * AuthorSearch shows how to query PDB structures by metadata.
 * This example queries the name fields in the audit_author and
 * citation_author categories.
 * <p>Example from 4P6I.cif
 * <pre>
 * loop_
 * _audit_author.name 
 * _audit_author.pdbx_ordinal 
 * ...
 * 'Doudna, J.A.'    4 
 * # 
 * loop_
 * _citation_author.citation_id 
 * _citation_author.name 
 * _citation_author.ordinal 
 * ...
 * primary 'Doudna, J.A.'    6
 * </pre>
 * 
 * Each category represents a table and fields represent database columns (see
 * <a href="https://pdbj.org/mine-rdb-docs">available tables and columns</a>).
 * 
 * <p> Data are provided through
 * <a href="https://pdbj.org/help/mine2-sql">Mine 2 SQL</a>
 * 
 * <p> Queries can be designed with the interactive 
 * <a href="https://pdbj.org/mine/sql">PDBj Mine 2
 * query service</a>.
 * 
 * @author Gert-Jan Bekker
 * @author Peter Rose
 * @since 0.2.0
 * 
 */
public class AuthorSearchDemo {
    public static void main(String[] args) throws IOException {
        
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(AuthorSearchDemo.class.getSimpleName());
        JavaSparkContext sc = new JavaSparkContext(conf);

        // query to find PDB structures for Doudna, J.A. as a deposition (audit) author
        // or as an author in the primary PDB citation
        String sqlQuery = "SELECT pdbid from audit_author "
                + "WHERE name LIKE 'Doudna%J.A.%' "
                + "UNION "
                + "SELECT pdbid from citation_author "
                + "WHERE citation_id = 'primary' AND name LIKE 'Doudna%J.A.%'";
        
        // read PDB and filter by author
        JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader
                .readReducedSequenceFile(sc)
                .filter(new PdbjMineSearch(sqlQuery));

        System.out.println("Number of entries matching query: " + pdb.count());

        sc.close();
    }
}
