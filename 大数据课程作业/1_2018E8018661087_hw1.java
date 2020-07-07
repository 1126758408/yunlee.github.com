import java.io.*;  
import java.net.URI;  
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Collections;
import java.util.Comparator;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
 
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;


/**
 * @author     Yun Lee <yunlee@whu.edu.cn>
 * @version    2019.0408
 * 
 * @classname : Hw1Grp1
 * @function discription : read two tables from HDFS, and then join them by sorting and merging, 
 * at last, write the result to Hbase
 */
public class Hw1Grp1 {

    /**
     * @param  TABLE_NAME       name of the result table
     * @param  COLUME_FAMILY    colume family of the result table
     */
	private static final String TABLE_NAME = "Result";
	private static final String COLUME_FAMILY = "res";


    /**
     * @usage : java Hw1Grp1 R=/hw1/lineitem.tbl S=/hw1/part.tbl join:R1=S0 res:S1,S3,R5
     * @return nothing
     * @exception IOException on input error.
     * @exception URISyntaxException throws exceptions when string could not be parsed as a URI reference.
     * @see IOException
     * @see URISyntaxException
     */
	public static void main(String[] args) throws IOException, URISyntaxException {
 		if (args.length != 4) {
 			System.out.println("Parameter Error!!");
 			System.out.println("Expected as:");
 			System.out.println("java Hw1Grp1 R=<file 1> S=<file 2> join:R2=S3 res:R4,S5");
 			System.exit(0);

 		}

 		String fileR = args[0].substring(args[0].indexOf('=')+1).trim();
 		System.out.println("fileR = " + fileR);
 		String fileS = args[1].substring(args[1].indexOf('=')+1).trim();

 		int joinKeyR = Integer.parseInt(args[2].substring(args[2].indexOf('R')+1, args[2].indexOf('=')).trim());
 		int joinKeyS = Integer.parseInt(args[2].substring(args[2].indexOf('S')+1).trim());

 		String[] resKey = args[3].substring(args[3].indexOf(':')+1).trim().split(",");

 		Hw1Grp1 hw1 = new Hw1Grp1();

 		hw1.createHtable();
 		hw1.sortMergeJoin(fileR, fileS, joinKeyR, joinKeyS, resKey);
 		System.out.println("Finish~~~~~~~");

	}


    /**
     * @function description : creat the buffer reader to read hdfs file
     * @param   file  the file path of the HDFS file to read
     * @return  buffer reader
     * @exception IOException on input error.
     * @exception URISyntaxException throws exceptions when string could not be parsed as a URI reference.
     * @see IOException
     * @see URISyntaxException
     */
	public static BufferedReader readFromHDFS(String file) throws IOException, URISyntaxException {
		Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(file), conf);
        Path path = new Path(file);
        FSDataInputStream in_stream = fs.open(path);

        BufferedReader in = new BufferedReader(new InputStreamReader(in_stream));
        return in;
	}


    /**
     * @function description : creat the result table, and create a htable to put result 
     * @return nothing
     * @exception MasterNotRunningException.
     * @exception IOException On input error.
     * @see MasterNotRunningException
     * @see IOException
     */
	public static void createHtable() throws MasterNotRunningException, IOException {

		// configure HBase
        Configuration configuration = HBaseConfiguration.create();
    	HBaseAdmin hAdmin = new HBaseAdmin(configuration);
    	
    	// create table name
    	TableName tn = TableName.valueOf(TABLE_NAME);

    	//if the table is existed?
    	if (hAdmin.tableExists(tn)) {
        	System.out.println("Table already exists!");
        	hAdmin.disableTable(tn);
			hAdmin.deleteTable(tn);
    	}
    	else {
        	System.out.println("table "+TABLE_NAME+ " is not existed!");
    	}

    	// create table descriptor
    	HTableDescriptor htd = new HTableDescriptor(tn);

    	// create column descriptor
    	HColumnDescriptor cf = new HColumnDescriptor(COLUME_FAMILY);
    	htd.addFamily(cf);

    	//create table
    	hAdmin.createTable(htd);

    	hAdmin.close();
	    //System.out.println("hhhhhhhhhhh");

	}


    /**
     * @function description : Read data from hdfs,use sort-merge join and store the result in HBase.
     * @param fileR the file path of the first data file
     * @param fileS the file path of the second data file
     * @param joinKeyR join key of the first data table.
     * @param joinKeyS join key of the second data table.
     * @param resKey result columns of the two tables.
     * @return nothing.
     * @exception IOException On input error.
     * @exception URISyntaxException throws exceptions when string could not be parsed as a URI reference.
     * @see IOException
     * @see URISyntaxException
     */
	public static void sortMergeJoin(String fileR, String fileS, int joinKeyR, int joinKeyS, String[] resKey) throws IOException, URISyntaxException {

		List<Integer> colR = new ArrayList<Integer>();//
		List<Integer> colS = new ArrayList<Integer>();
		List<String> resColName = new ArrayList<String>();//

		for (int i = 0; i < resKey.length; i++) {
			if (resKey[i].indexOf('R') == 0) {
				colR.add(Integer.parseInt(resKey[i].substring(1)));
			}
			else if (resKey[i].indexOf('S') == 0) {
				colS.add(Integer.parseInt(resKey[i].substring(1)));
			}
		}

		for (int i = 0; i < colR.size(); i++) {
			resColName.add("R" + String.valueOf(colR.get(i)));
		}
		for (int i = 0; i < colS.size(); i++) {
			resColName.add("S" + String.valueOf(colS.get(i)));
		}

		BufferedReader bufR= readFromHDFS(fileR);
 		BufferedReader bufS= readFromHDFS(fileS);

		String s;
		List<String[]> tupleR = new ArrayList<String[]>();
		List<String[]> tupleS = new ArrayList<String[]>();
        
        while ((s = bufR.readLine()) != null) {

        	String[] tmpTupleR = s.split("\\|");
        	tupleR.add(tmpTupleR);
        }
        while((s = bufS.readLine()) != null) {

        	String[] tmpTupleS = s.split("\\|");
        	tupleS.add(tmpTupleS);
        }

        Comparator<String[]> comparatorR = new Comparator<String[]>() {
        	public int compare(String[] tuple1, String[] tuple2) {
        		return(tuple1[joinKeyR].compareTo(tuple2[joinKeyR]));
        	}
        };

        Comparator<String[]> comparatorS = new Comparator<String[]>() {
        	public int compare(String[] tuple1, String[] tuple2) {
        		return(tuple1[joinKeyS].compareTo(tuple2[joinKeyS]));
        	}
        };

        //sort
        Collections.sort(tupleR, comparatorR);
        Collections.sort(tupleS, comparatorS);

        int numR = tupleR.size();
        int numS = tupleS.size();

        int countR = 0;
        int countS = 0;

        String[] temResR = new String[colR.size() + 1];
        String[] temResS = new String[colS.size() + 1];
        String[] lastResS = new String[colS.size() + 1];

        String lastRowKey = null;
        String indexFlag = "";
        int index = 0;
        String rowKey = null;
        Put put = null;
        HTable table = new HTable(HBaseConfiguration.create(),TABLE_NAME);
        //List<Integer> joinList = new ArrayList<Integer>();

        for (countR = 0; countR < numR; countR++) {
            List<Integer> joinList = new ArrayList<Integer>();

        	if (tupleR.get(countR)[joinKeyR].compareTo(tupleS.get(countS)[joinKeyS]) > 0) {
        		countS ++;
                countR --;
        		if (countS >= numS) {
        			break;
        		}
        		
        	}
        	else if (tupleR.get(countR)[joinKeyR].compareTo(tupleS.get(countS)[joinKeyS]) == 0) {

        		int temCount = countS;

        		joinList.add(temCount);
        		temCount ++;
        		if ((temCount + 1) <= numS) {
        			while(tupleR.get(countR)[joinKeyR].compareTo(tupleS.get(temCount)[joinKeyS]) == 0) {
        				joinList.add(temCount);
        				temCount ++;
        				if ((temCount + 1) > numS) {
        					break;
        				}
        			}
        		}

        		rowKey = tupleR.get(countR)[joinKeyR];
        		int joinListNum = joinList.size();
        		
        		temResR[0] = rowKey;
        		for (int i = 1; i <= colR.size(); i++) {
        			temResR[i] = tupleR.get(countR)[colR.get(i-1)];
        		}
        		
        		
        		temResS[0] = rowKey;
        		for (int i = 1; i <= colS.size(); i++) {
        			temResS[i] = tupleS.get(countS)[colS.get(i-1)];
        		}

        		if (rowKey != lastRowKey) {
        			index = 0;
        			indexFlag = "";
        			put = new Put(rowKey.getBytes());
        			for(int i = 1; i <= colR.size(); i++) {
						put.add(COLUME_FAMILY.getBytes(),(resColName.get(i-1)).getBytes(),temResR[i].getBytes());
					}
					table.put(put);
					put = new Put(rowKey.getBytes());
        			for(int i = 1; i <= colS.size(); i++) {
						put.add(COLUME_FAMILY.getBytes(),(resColName.get(colR.size()+i-1)).getBytes(),temResS[i].getBytes());
					}
					table.put(put);

					if (joinList.size() > 1) {
						for (int j = 1; j < joinList.size(); j++) {
							index ++;
							indexFlag = "." + String.valueOf(index);
							put = new Put(rowKey.getBytes());
        					for(int i = 1; i <= colR.size(); i++) {
								put.add(COLUME_FAMILY.getBytes(),(resColName.get(i-1) + indexFlag).getBytes(),temResR[i].getBytes());
							}
							table.put(put);
							for (int i = 1; i <= colS.size(); i++) {
        						temResS[i] = tupleS.get(joinList.get(j))[colS.get(i-1)];
        					}
							put = new Put(rowKey.getBytes());
        					for(int i = 1; i <= colS.size(); i++) {
								put.add(COLUME_FAMILY.getBytes(),(resColName.get(colR.size()+i-1) + indexFlag).getBytes(),temResS[i].getBytes());
							}
							table.put(put);
						}
						
					}

        		}
        		else{
        			for (int j = 0; j < joinList.size(); j++) {
        				index ++;
						indexFlag = "." + String.valueOf(index);
						put = new Put(rowKey.getBytes());
        				for(int i = 1; i <= colR.size(); i++) {
							put.add(COLUME_FAMILY.getBytes(),(resColName.get(i-1) + indexFlag).getBytes(),temResR[i].getBytes());
						}
						table.put(put);
						for (int i = 1; i <= colS.size(); i++) {
       						temResS[i] = tupleS.get(joinList.get(j))[colS.get(i-1)];
       					}
						put = new Put(rowKey.getBytes());        					
						for(int i = 1; i <= colS.size(); i++) {
							put.add(COLUME_FAMILY.getBytes(),(resColName.get(colR.size()+i-1) + indexFlag).getBytes(),temResS[i].getBytes());
						}
						table.put(put);
        			}

        		}
        		//table.close();
    			System.out.println("put successfully");

        		lastRowKey = rowKey;
        	}
        }
        table.close();		

	}

}
