package org.apache.hadoop.hive.contrib.fileformat.netcdf;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseHelper {

	private Configuration conf = null;
	private HTable table = null;
	private Result rs = null;

	/*
	 * Initialization
	 */
	public static String MD5(String md5) {
		try {
			java.security.MessageDigest md = java.security.MessageDigest
					.getInstance("MD5");
			byte[] array = md.digest(md5.getBytes());
			StringBuffer sb = new StringBuffer();
			for (int i = 0; i < array.length; ++i) {
				sb.append(Integer.toHexString((array[i] & 0xFF) | 0x100)
						.substring(1, 3));
			}
			return sb.toString();
		} catch (java.security.NoSuchAlgorithmException e) {
		}
		return null;
	}

	public HBaseHelper(String tableName, String[] familys) {
		try {
			conf = HBaseConfiguration.create();

			createTable(tableName, familys);
			table = new HTable(conf, tableName);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public boolean hasTable() {
		return table == null ? false : true;
	}

	/*
	 * Create a table
	 */
	public boolean createTable(String tableName, String[] familys)
			throws Exception {
		HBaseAdmin admin = new HBaseAdmin(conf);
		if (admin.tableExists(tableName)) {
			// System.out.println("table already exists!");
			return false;
		} else {
			HTableDescriptor tableDesc = new HTableDescriptor(tableName);
			for (int i = 0; i < familys.length; i++) {
				tableDesc.addFamily(new HColumnDescriptor(familys[i]));
			}
			admin.createTable(tableDesc);
			// System.out.println("create table " + tableName + " ok.");
			return true;
		}
	}

	/*
	 * Delete a table
	 */
	public void deleteTable(String tableName) throws Exception {
		try {
			HBaseAdmin admin = new HBaseAdmin(conf);
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
			table = null;
			// System.out.println("delete table " + tableName + " ok.");
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		}
	}

	/*
	 * insert a row
	 */
	public void addRecord(String rowKey, String family, String qualifier,
			String value) throws Exception {
		try {
			// HTable table = new HTable(conf, tableName);
			Put put = new Put(Bytes.toBytes(rowKey));
			put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier),
					Bytes.toBytes(value));
			table.put(put);
			// System.out.println("insert recored " + rowKey + " to table "+
			// tableName + " ok.");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/*
	 * insert a row
	 */
	public void addRecord(String rowKey, String family, String qualifier,
			byte[] value) throws Exception {
		try {
			// HTable table = new HTable(conf, tableName);
			Put put = new Put(Bytes.toBytes(rowKey));
			put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), value);
			table.put(put);
			// System.out.println("insert recored " + rowKey + " to table "+
			// tableName + " ok.");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/*
	 * delete a row
	 */
	public void deleteRecord(String rowKey) throws IOException {
		// HTable table = new HTable(conf, tableName);
		List<Delete> list = new ArrayList<Delete>();
		Delete del = new Delete(rowKey.getBytes());
		list.add(del);
		table.delete(list);
		// System.out.println("del recored " + rowKey + " ok.");
	}

	/*
	 * Get a row
	 */
	public boolean getOneRecord(String rowKey) throws IOException {
		// HTable table = new HTable(conf, tableName);
		Get get = new Get(rowKey.getBytes());
		rs = table.get(get);
		if (rs.isEmpty())
			return false;
		return true;
		/*
		 * for(KeyValue kv : rs.raw()){ System.out.print(new String(kv.getRow())
		 * + " " ); System.out.print(new String(kv.getFamily()) + ":" );
		 * System.out.print(new String(kv.getQualifier()) + " " );
		 * System.out.print(kv.getTimestamp() + " " ); System.out.println(new
		 * String(kv.getValue())); } return true;
		 */
	}

	public Result getResult() {
		return rs;
	}

	/*
	 * scan a table
	 */
	public void getAllRecord() throws IOException {

		// HTable table = new HTable(conf, tableName);
		Scan s = new Scan();
		ResultScanner rs = table.getScanner(s);
		for (Result r : rs) {
			for (KeyValue kv : r.raw()) {
				System.out.print(new String(kv.getRow()) + " ");
				System.out.print(new String(kv.getFamily()) + ":");
				System.out.print(new String(kv.getQualifier()) + " ");
				System.out.print(kv.getTimestamp() + " ");
				System.out.println(new String(kv.getValue()));
			}
		}
	}

	public static byte[] serialize(Object obj) throws IOException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		ObjectOutputStream os = new ObjectOutputStream(out);
		os.writeObject(obj);
		return out.toByteArray();
	}

	public static Object deserialize(byte[] data) throws IOException,
			ClassNotFoundException {
		ByteArrayInputStream in = new ByteArrayInputStream(data);
		ObjectInputStream is = new ObjectInputStream(in);
		return is.readObject();
	}

	public static void main(String[] agrs) {
		try {
			String tablename = "scores";
			String[] familys = { "grade", "course" };
			HBaseHelper hh = new HBaseHelper(tablename, familys);
			hh.createTable(tablename, familys);

			// add record zkb
			hh.addRecord("zkb", "grade", "", "5");
			hh.addRecord("zkb", "course", "", "90");
			hh.addRecord("zkb", "course", "math", "97");
			hh.addRecord("zkb", "course", "art", "87");
			// add record baoniu
			hh.addRecord("baoniu", "grade", "", "4");
			hh.addRecord("baoniu", "course", "math", "89");

			System.out.println("===========get one record========");
			hh.getOneRecord("zkb");

			System.out.println("===========show all record========");
			hh.getAllRecord();

			System.out.println("===========del one record========");
			hh.deleteRecord("baoniu");
			hh.getAllRecord();

			System.out.println("===========show all record========");
			hh.getAllRecord();
			System.out.println("===========delete all record========");
			hh.deleteTable(tablename);
			hh.getOneRecord("zkb");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}