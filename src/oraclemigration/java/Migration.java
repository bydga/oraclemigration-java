/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package oraclemigration.java;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.ini4j.Wini;

public class Migration {

	private static String tableQuery = "select * from information_schema.tables where table_schema = 'public' and table_catalog='eeg' order by table_name";
	private Connection oracleConnection;
	private Connection pgConnection;
	private List<String> skipTables;
	private List<String> bools;
	private List<String> blobs;

	public Migration() throws Exception {
		Wini ini = new Wini(new File("config.ini"));
		System.out.println(ini.toString());

		String oracleString = ini.get("oracle", "url");
		String oracleUserString = ini.get("oracle", "user");
		String oraclePassString = ini.get("oracle", "pass");

		String pgString = ini.get("pg", "url");
		String pgUserString = ini.get("pg", "user");
		String pgPassString = ini.get("pg", "pass");

		this.skipTables = Arrays.asList(ini.get("info", "skip").split(","));
		this.bools = Arrays.asList(ini.get("info", "bools").split(","));
		this.blobs = Arrays.asList(ini.get("info", "blobs").split(","));


		oracleConnection = DriverManager.getConnection(oracleString, oracleUserString, oraclePassString);
		pgConnection = DriverManager.getConnection(pgString, pgUserString, pgPassString);
		pgConnection.setAutoCommit(false);
	}

	public void migrate() throws Exception {


		//get all tables
		List<HashMap<String, Object>> result = query(pgConnection, tableQuery);
		for (HashMap<String, Object> table : result) {
			String tableName = (String) table.get("table_name");
			if (this.skipTables.contains(tableName)) {
				continue;
			}
			List<HashMap<String, Object>> oracleResult = query(oracleConnection, "SELECT * from " + tableName);

			System.out.print("inserting " + oracleResult.size() + " into " + tableName + " ");

			for (HashMap<String, Object> row : oracleResult) {
				int cnt = 0;
				String insert = "INSERT INTO " + tableName + " (";
				for (Map.Entry<String, Object> entry : row.entrySet()) {
					insert += entry.getKey() + ",";
					cnt++;
				}
				insert = insert.substring(0, insert.length() - 1) + ") VALUES (";
				for (int i = 0; i < cnt; i++) {
					insert += " ?,";
				}
				insert = insert.substring(0, insert.length() - 1) + " )";
				PreparedStatement prepareStatement = pgConnection.prepareStatement(insert);
				int i = 1;
				for (Map.Entry<String, Object> entry : row.entrySet()) {
					Object val = entry.getValue();
					String id = tableName.toLowerCase() + "." + entry.getKey().toLowerCase();
					if (bools.contains(id)) {

						val = val.toString().equals("1") ? true : false;
						prepareStatement.setBoolean(i, (Boolean) val);
					} else if (blobs.contains(id)) {
						Blob b = (Blob) val;
						if (b == null) {
							prepareStatement.setObject(i, null);
						} else {
							byte[] data = b.getBytes(1, (int) b.length());
							prepareStatement.setBinaryStream(i, new ByteArrayInputStream(data), data.length);
						}
					} else {

						if (val instanceof java.sql.Timestamp) {

							if (val.toString().equals("5500-02-23 11:30:00.0")) {
								val = new java.sql.Timestamp(946684800000L);
							}
						}
						prepareStatement.setObject(i, val);
					}

					i++;
				}
				System.out.print("#");
				prepareStatement.executeUpdate();

			}
			System.out.println();
		}
		pgConnection.commit();


		System.out.println("now migrating scenario files");
		List<HashMap<String, Object>> query = query(oracleConnection, "SELECT * from SCENARIO_TYPE_NONXML");
		System.out.println("count: " + query.size());
		for (HashMap<String, Object> row : query) {
			PreparedStatement prepareStatement = pgConnection.prepareStatement("UPDATE SCENARIO SET SCENARIO_FILE=? WHERE SCENARIO_ID=?");
			Blob b = (Blob) row.get("SCENARIO_XML");
			System.out.print("#");
			if (b == null) {
				continue;
			}
//			byte[] data = b.getBytes(1, (int) b.length());
//			prepareStatement.setBinaryStream(1, new ByteArrayInputStream(data), data.length);
			prepareStatement.setObject(1, b);

			prepareStatement.setObject(2, row.get("SCENARIO_ID"));
			prepareStatement.executeUpdate();
		}
		System.out.println("\ndone");
		pgConnection.commit();

	}

	private void trigger(String state) throws Exception {
		List<HashMap<String, Object>> result = query(pgConnection, tableQuery);
		for (HashMap<String, Object> table : result) {
			String tableName = (String) table.get("table_name");
			String alter = "alter table " + tableName + " " + state + " trigger all";
			Statement stmt = pgConnection.createStatement();
			stmt.execute(alter);
			pgConnection.commit();
		}
	}

	public void enable() throws Exception {
		this.trigger("enable");
	}

	public void disable() throws Exception {
		this.trigger("disable");
	}

	public void truncate() throws Exception {
		List<HashMap<String, Object>> result = this.query(pgConnection, tableQuery);
		for (HashMap<String, Object> table : result) {
			String tableName = (String) table.get("table_name");
			if (this.skipTables.contains(tableName)) {
				continue;
			}
			String truncate = "truncate " + tableName + " cascade";
			System.out.println(truncate);
			Statement stmt = pgConnection.createStatement();
			stmt.execute(truncate);
			pgConnection.commit();
		}
	}

	public static void main(String[] args) throws Exception {
		Class.forName("oracle.jdbc.OracleDriver");
		Class.forName("org.postgresql.Driver");
		Migration m = new Migration();

		if (args.length != 1) {
			System.out.println("missing command");
			return;
		}
		Migration.class.getMethod(args[0]).invoke(m);

	}

	//query a full sql command
	private List<HashMap<String, Object>> query(Connection conn, String command) throws Exception {
		Statement stmt = conn.createStatement();

		ResultSet result;
		boolean returningRows = stmt.execute(command);
		if (returningRows) {
			result = stmt.getResultSet();
		} else {
			return new ArrayList<HashMap<String, Object>>();
		}

		ResultSetMetaData meta = result.getMetaData();

		int colCount = meta.getColumnCount();
		ArrayList<String> cols = new ArrayList<String>();
		for (int Index = 1; Index <= colCount; Index++) {
			cols.add(meta.getColumnName(Index));
		}

		ArrayList<HashMap<String, Object>> rows = new ArrayList<HashMap<String, Object>>();

		while (result.next()) {
			HashMap<String, Object> row = new HashMap<String, Object>();
			for (String colName : cols) {
				Object Val = result.getObject(colName);
				row.put(colName, Val);
			}
			rows.add(row);
		}

		//pass back rows
		return rows;
	}
}