package edu.sdsc.mmtf.spark.io;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.rcsb.mmtf.api.StructureDataInterface;

/**
 * 
 * @author Yue Yu
 */
public class HadoopUpdate {
	
	private static String ftpServer = "ftp://ftp.wwpdb.org/pub/pdb/data/status/";
	
	private static List<String> removeDup(List<String> dup)
	{
		return dup.stream()
			     .distinct()
			     .collect(Collectors.toList());
	}
	
	private static List<String> getAdded(String date)
	{
		List<String> tmp = new ArrayList<String>();
		try {
			URL url = new URL(ftpServer + date + "/added.pdb");
			URLConnection conn = url.openConnection();
			InputStream inputStream = conn.getInputStream();
	        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
	        String line;
            while ((line = reader.readLine()) != null) {
            	tmp.add(line);
            }
            inputStream.close();
		}catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return tmp;
	}
	private static List<String> getModified(String date)
	{
		List<String> tmp = new ArrayList<String>();
		try {
			URL url = new URL(ftpServer + date + "/modified.pdb");
			URLConnection conn = url.openConnection();
			InputStream inputStream = conn.getInputStream();
	        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
	        String line;
            while ((line = reader.readLine()) != null) {
            	tmp.add(line);
            }
            inputStream.close();
		}catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return tmp;
	}
	private static List<String> getObsolete(String date)
	{
		List<String> tmp = new ArrayList<String>();
		try {
			URL url = new URL(ftpServer + date + "/obsolete.pdb");
			URLConnection conn = url.openConnection();
			InputStream inputStream = conn.getInputStream();
	        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
	        String line;
            while ((line = reader.readLine()) != null) {
            	tmp.add(line);
            }
            inputStream.close();
		}catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return tmp;
	}
	
	private static List<List<String>> getLists(String date)
	{
		List<List<String>> tmp = new ArrayList<List<String>>();
		List<String> addedList = new ArrayList<String>();
		List<String> removedList = new ArrayList<String>();
		try {
			URL url = new URL(ftpServer);
			URLConnection conn = url.openConnection();
			InputStream inputStream = conn.getInputStream();
	        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
	        System.out.println("--- START PULLING DATA ---");
	        String line;
            while ((line = reader.readLine()) != null) {
            	String lineDate = line.substring(line.length()-8);
            	String todayDate = new SimpleDateFormat("yyyyMMdd").format(new Date());
            	if( lineDate.compareTo(date) >= 0 && lineDate.compareTo(todayDate) < 0)
            	{
            		System.out.println("Reading data for: " + lineDate);
            		for (String x : getObsolete(lineDate))
            		{
            			if(addedList.contains(x.toUpperCase()))	addedList.remove(x.toUpperCase());
            			else removedList.add(x.toUpperCase());
            		}
            		for (String x : getModified(lineDate))
            			if(!addedList.contains(x.toUpperCase()))
            			{
            				addedList.add(x.toUpperCase());
            				removedList.add(x.toUpperCase());
            			}
          		
            		for (String x : getAdded(lineDate))
            			addedList.add(x.toUpperCase());
            	}
            }
            System.out.println("--- END PULLING DATA ---");
            inputStream.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        tmp.add(removeDup(addedList));
        tmp.add(removeDup(removedList));
		return tmp;
	}
	
	private static String getDate(String path){
		File folder = new File(path);
		File[] listOfFiles = folder.listFiles();

		for (int i = 0; i < listOfFiles.length; i++) {
			if (listOfFiles[i].isFile() && listOfFiles[i].getName().endsWith(".txt")) {
				String theDate = listOfFiles[i].getName();
				//if it is original data
				if(theDate.length() == 15)				
					return theDate.substring(1,5).concat(theDate.substring(6,8).concat(theDate.substring(9,11)));
				//if it is local updated data
				else if(theDate.length() == 21)
					return theDate.substring(7,11).concat(theDate.substring(12,14).concat(theDate.substring(15,17)));
		    }
		}
		System.out.println("Reduced hadoop sequence files failed to update: " + "Date Info Not Found");
		return "";
		    
	}
	
	public static void performUpdate(JavaSparkContext sc) throws FileNotFoundException{

		try{
			String reducedPath = MmtfReader.getMmtfReducedPath();
			String latestDate = getDate(reducedPath);
			System.out.println("Current Last Updated Date: " + latestDate);
			if(getDate(reducedPath) == "")	return;
			
			List<List<String>> tmp = getLists(latestDate);
			List<String> addedList = tmp.get(0);
			List<String> removedList = tmp.get(1);
			
			JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader.readSequenceFile(reducedPath, sc);
			pdb = pdb.filter(t->!removedList.contains(t._1));
			JavaPairRDD<String, StructureDataInterface> newPdb = MmtfReader.downloadReducedMmtfFiles(addedList, sc);
			pdb = pdb.union(newPdb);

			MmtfWriter.writeSequenceFile(reducedPath + "_new", sc, pdb);
			String todayDate = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
			File file = new File(reducedPath + "_new/_local_" + todayDate + ".txt");
			file.createNewFile();
			System.out.println("Reduced hadoop sequence files update success! File generated at: " + reducedPath);
				
			//below substitute the old version by the new version		
			File root = new File(reducedPath);
			for(File tempFile : root.listFiles()) {
			    tempFile.delete();
			}
			root.delete();			
			File dir = new File(reducedPath + "_new");
			File fList[] = dir.listFiles();
			for (File f : fList) 
			    if (f.getName().endsWith(".crc"))
			        f.delete();
            File newName = new File(reducedPath);
            dir.renameTo(newName);      
            
		}catch(Exception e)
		{
			System.out.println("Reduced hadoop sequence files failed to update: " + e);
		};
		
		try{
			String fullPath = MmtfReader.getMmtfFullPath();
			String latestDate = getDate(fullPath);
			System.out.println("Current Last Updated Date: " + latestDate);
			if(getDate(fullPath) == "")	return;
			
			List<List<String>> tmp = getLists(latestDate);
			List<String> addedList = tmp.get(0);
			List<String> removedList = tmp.get(1);
			
			JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader.readSequenceFile(fullPath, sc);
			pdb = pdb.filter(t->!removedList.contains(t._1));
			JavaPairRDD<String, StructureDataInterface> newPdb = MmtfReader.downloadFullMmtfFiles(addedList, sc);
			pdb = pdb.union(newPdb);

			MmtfWriter.writeSequenceFile(fullPath + "_new", sc, pdb);
			String todayDate = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
			File file = new File(fullPath + "_new/_local_" + todayDate + ".txt");
			file.createNewFile();
			System.out.println("Full hadoop sequence files update success! File generated at: " + fullPath);
				
			//below substitute the old version by the new version		
			File root = new File(fullPath);
			for(File tempFile : root.listFiles()) {
			    tempFile.delete();
			}
			root.delete();			
			File dir = new File(fullPath + "_new");
			File fList[] = dir.listFiles();
			for (File f : fList) 
			    if (f.getName().endsWith(".crc"))
			        f.delete();
            File newName = new File(fullPath);
            dir.renameTo(newName);           
            
		}catch(Exception e)
		{
			System.out.println("Full hadoop sequence files failed to update: " + e);
		};
		
	}
}
