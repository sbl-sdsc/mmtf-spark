package edu.sdsc.mmtf.spark.incubator;

import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;
import org.rcsb.mmtf.api.StructureDataInterface;
import org.rcsb.mmtf.encoder.ReducedEncoder;

import edu.sdsc.mmtf.spark.filters.demos.FilterByRFree;
import edu.sdsc.mmtf.spark.io.MmtfReader;

public class ReducedEncoderNewTest {

	@Test
	public void test() {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(FilterByRFree.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
		 
//	    List<String> pdbIds = Arrays.asList("1STP","4HHB","2ONX","1JLP","5X6H","5L2G","2MK1");
	    List<String> pdbIds = Arrays.asList("1STP","4HHB","2ONX","2CCV");
	    JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader.downloadMmtfFiles(pdbIds, sc).cache();	    
	    pdb.foreach(t -> System.out.println(t._1 + "o :" + t._2.getNumBonds()));
	 
	    List<String> chainIds = pdb.map(t -> t._1 + "_chainId_" + Arrays.toString(t._2.getChainIds())).collect();
	    System.out.println("full: " + chainIds);
	    
	    List<String> chainNames = pdb.map(t -> t._1 + "_chainNames_" + Arrays.toString(t._2.getChainNames())).collect();
	    System.out.println("full: " + chainNames);
	    List<String> numGroups = pdb.map(t -> t._1 + "_numGroups_" + t._2.getNumGroups()).collect();
	    System.out.println("full: " + numGroups);
	    List<String> altlocs = pdb.map(t -> t._1 + "_altLocs_" + Arrays.toString(t._2.getAltLocIds())).collect();
	    System.out.println("full: " + altlocs);
	    
	    pdb = pdb.mapValues(v -> ReducedEncoder.getReduced(v)).cache();

	    chainIds = pdb.map(t -> t._1 + "_chainId_" + Arrays.toString(t._2.getChainIds())).collect();
	    System.out.println("reduced: " + chainIds);
	    chainNames = pdb.map(t -> t._1 + "_chainNames_" + Arrays.toString(t._2.getChainNames())).collect();
	    System.out.println("reduced: " + chainNames);
	    altlocs = pdb.map(t -> t._1 + "_altLocs_" + Arrays.toString(t._2.getAltLocIds())).collect();
	    System.out.println("reduced: " + altlocs);
	    
	    
	    // 1STP # groups 121 CA + 1 BTN = 122
	    // 4HHB # groups 141x2 + 146x2 CA +  4 HEM + 2P (from PO4) = 580
	    // 2ONX # groups 4 CA = 4
	    // 2CVV # atoms 99 CA + 4 altloc CA + 1 A2G (sugar) + 1 NAG (orig 15) + 1 GOL + 1 ZN, 1 ACE = 108 
	    // TODO (4 altlocs missing?)
	    
	    numGroups = pdb.map(t -> t._1 + "_numGroups_" + t._2.getNumGroups()).collect();
	    System.out.println("reduced: " + numGroups);
	    
	    List<String> atoms = pdb.map(t -> t._1 + "_atoms_" + t._2.getNumAtoms()).collect();
	    System.out.println(atoms);
	    // 1STP # atoms 121 CA + 16 BTN
	    // 4HHB # atom 141x2 + 146x2 CA +  43x4 HEM + 2P (from PO4) = 748
	    // 2ONX # atoms 4 CA
	    // 2CVV # atoms 99 CA + 4 (5?) altloc CA + 15 A2G (sugar) + 14 NAG (orig 15) + 6 GOL + 1 ZN, ACE 4 = 143
        assertTrue(atoms.contains("1STP_atoms_137"));
        assertTrue(atoms.contains("4HHB_atoms_748"));
        assertTrue(atoms.contains("2ONX_atoms_4"));
        assertTrue(atoms.contains("2CCV_atoms_143"));
        
	    List<String> bonds = pdb.map(t -> t._1 + "_bonds_" + t._2.getNumBonds()).collect();
	    // 1STP # bond 17 BTN
	    // 4HHB # bonds 50 x 4 HEM = 200
	    // 2ONX # bonds 0
	    // 2CVV # bonds 15 A2G+ 14 NAG (-O) + 5 GOL + 3 ACE + 2 disulfide bridges + 1 covalent bond to NAG = 40
        assertTrue(bonds.contains("1STP_bonds_17"));
        assertTrue(bonds.contains("4HHB_bonds_200"));
        assertTrue(bonds.contains("2ONX_bonds_0"));
        assertTrue(bonds.contains("2CCV_bonds_40"));
		
		sc.close();
	}
}
