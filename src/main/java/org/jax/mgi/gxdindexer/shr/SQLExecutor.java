package org.jax.mgi.gxdindexer.shr;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.Properties;

import org.jax.mgi.gxdindexer.Indexer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The SQLExecutor class knows how to create connections to MGD. It can also
 * clean up the connections after they are done, and execute queries against a
 * given database.
 * 
 * The class is also smart enough to only open connections when they are needed.
 * If we go to run a new query and a connection hasn't been created yet, we
 * create one.
 * 
 * @author mhall
 * 
 * @does Executes SQL Queries against either the MGD Database.
 *
 */

public class SQLExecutor {

	private Logger logger = LoggerFactory.getLogger(this.getClass());
	public Properties props = new Properties();
	protected Connection conMGD = null;
	private String user;
	private String password;
	private String mgdJDBCUrl;

	private Date start;
	private Date end;

	/**
	 * The default constructor pulls in connection information from the property
	 * files.
	 * 
	 * @param config
	 */

	public SQLExecutor() {
		try {

			InputStream in = Indexer.class.getClassLoader().getResourceAsStream("config.props");
			try {
				props.load(in);
			} catch (IOException e1) {
				e1.printStackTrace();
			}

			Class.forName(props.getProperty("database.JDBC.driver"));
			user = props.getProperty("mgd.user");
			password = props.getProperty("mgd.password");
			mgdJDBCUrl = props.getProperty("mgd.JDBC.url");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Sets up the connection to the MGD Database.
	 * 
	 * @throws SQLException
	 */

	private void getMGDConnection() throws SQLException {
		conMGD = DriverManager.getConnection(mgdJDBCUrl, user, password);
		conMGD.setAutoCommit(false);
	}

	/**
	 * Clean up the connections to the database, if they have been initialized.
	 * 
	 * @throws SQLException
	 */

	public void cleanup() throws SQLException {
		if (conMGD != null) {
			conMGD.close();
		}
	}

	/**
	 * Execute a statement against MGD (where that statement has no rows returned),
	 * setting up the connection if needed.
	 * 
	 * @param query
	 */
	public void executeUpdate(String cmd) {

		try {
			if (conMGD == null) {
				getMGDConnection();
			}

			java.sql.Statement stmt = conMGD.createStatement();
			start = new Date();
			logger.info(cmd);
			stmt.executeUpdate(cmd);
			end = new Date();
			logger.info("Query took: " + getTimestamp());
			return;
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
			return;
		}
	}

	/*
	 * execute any SQL that does not return a result
	 */
	public void executeVoid(String sql) {
		try {
			if (conMGD == null)
				getMGDConnection();
			java.sql.Statement stmt = conMGD.createStatement();
			start = new Date();
			logger.info(sql);
			stmt.execute(sql);
			end = new Date();
			logger.info("Query took: " + getTimestamp());
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

	/**
	 * Execute a query against MGD, setting up the connection if needed.
	 * 
	 * @param query
	 * @return
	 */
	public ResultSet executeProto(String query) {
		return executeProto(query, 10000);
	}

	/**
	 * Execute a query against MGD, setting up the connection if needed. Use a
	 * cursor to return 'cursorLimit' results at a time.
	 * 
	 * @param query
	 * @return
	 */
	public ResultSet executeProto(String query, int cursorLimit) {

		ResultSet set;

		try {
			if (conMGD == null) {
				getMGDConnection();
			}

			java.sql.Statement stmt = conMGD.createStatement();
			if (cursorLimit > 0) {
				stmt.setFetchSize(cursorLimit);
			}
			logger.info(query);
			start = new Date();
			set = stmt.executeQuery(query);
			end = new Date();
			logger.info("Query took: " + getTimestamp());
			return set;
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
			return null;
		}
	}

	/**
	 * Return the timing of the last query.
	 * 
	 * @return
	 */

	public long getTiming() {
		return end.getTime() - start.getTime();
	}

	/*
	 * returns a formatted timestamp as a string, showing the last query's execution
	 * time in ms. format: "(n ms)"
	 */
	public String getTimestamp() {
		return getTiming() + " ms";
	}

	@Override
	public String toString() {
		return "SQLExecutor[user=" + user + ",password=" + password + ",url=" + mgdJDBCUrl + "]";
	}

}
