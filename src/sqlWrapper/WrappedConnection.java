/*
 * Copyright 2007-2011 Daniel Armbrust 
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License. 
 * You may obtain a copy of the License at
 * 
 *  http://www.apache.org/licenses/LICENSE-2.0
 *   
 * Unless required by applicable law or agreed to in writing, software 
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 * See the License for the specific language governing permissions and 
 * limitations under the License.
 */
package sqlWrapper;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * This class in combination with the WrappedPreparedStatement give you an automatically
 * reconnecting sql connection. If a failure occurs (due to timeout) when you execute a prepared
 * statement, it will create a new connection, a new prepared statement, and reexecute the query.
 * 
 * It also gives you a nice toString implementation for Prepared Statements (including set values of
 * variables)
 * 
 * Hasn't really been maintained or tested by me in quite some time. But folks are still finding it
 * useful. So this is a release that compiles against Java 1.6.
 * 
 * You should get this code from:
 * http://code.google.com/p/armbrust-file-utils/source/browse/#svn%2Ftrunk%2FSQLWrapper-1.6
 * 
 * @author <A HREF="mailto:daniel.armbrust@gmail.com">Dan Armbrust</A>
 */

public class WrappedConnection implements Connection
{
    protected Connection                        connection_;
    private String                              userName_;
    private String                              password_;
    private String                              driver_;
    private String                              server_;
    private boolean                             useUTF8_      = false;

    private int                                 maxFailCount_ = 3;

    private Log logger        = LogFactory.getLog("sqlWrapper.WrappedConnection");

    public WrappedConnection(String userName, String password, String driver, String server)
            throws ClassNotFoundException, SQLException
    {
        logger.debug("Creating a new reconnectable SQL connection to " + server);
        userName_ = userName;
        password_ = password;
        driver_ = driver;
        server_ = server;
        useUTF8_ = false;

        if (userName_ == null)
        {
            userName_ = "";
        }
        if (password_ == null)
        {
            password_ = "";
        }
        connect();
    }

    public WrappedConnection(String userName, String password, String driver, String server, boolean useUTF8)
            throws ClassNotFoundException, SQLException
    {
        logger.debug("Creating a new reconnectable SQL connection to " + server);
        userName_ = userName;
        password_ = password;
        driver_ = driver;
        server_ = server;
        useUTF8_ = useUTF8;

        if (userName_ == null)
        {
            userName_ = "";
        }
        if (password_ == null)
        {
            password_ = "";
        }
        connect();
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException
    {
        logger.debug("Creating reconnectable prepared statement: \"" + sql + "\"");
        return new WrappedPreparedStatement(this, sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
            throws SQLException
    {
        logger.debug("Creating reconnectable prepared statement: \"" + sql + "\"");
        return new WrappedPreparedStatement(this, sql, resultSetType, resultSetConcurrency);
    }

    @Override
    public int getHoldability() throws SQLException
    {
        return connection_.getHoldability();
    }

    @Override
    public int getTransactionIsolation() throws SQLException
    {
        return connection_.getTransactionIsolation();
    }

    @Override
    public void clearWarnings() throws SQLException
    {
        connection_.clearWarnings();
    }

    @Override
    public void close() throws SQLException
    {
        connection_.close();
    }

    @Override
    public void commit() throws SQLException
    {
        connection_.commit();
    }

    @Override
    public void rollback() throws SQLException
    {
        connection_.rollback();
    }

    @Override
    public boolean getAutoCommit() throws SQLException
    {
        return connection_.getAutoCommit();
    }

    @Override
    public boolean isClosed() throws SQLException
    {
        return connection_.isClosed();
    }

    @Override
    public boolean isReadOnly() throws SQLException
    {
        return connection_.isReadOnly();
    }

    @Override
    public String getCatalog() throws SQLException
    {
        return connection_.getCatalog();
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException
    {
        return connection_.getMetaData();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException
    {
        return connection_.getWarnings();
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException
    {
        connection_.rollback(savepoint);
    }

    private Integer holdability_;
    @Override
    public void setHoldability(int holdability) throws SQLException
    {
        connection_.setHoldability(holdability);
        holdability_ = new Integer(holdability);
    }

    private Integer transactionIsoloation_;
    @Override
    public void setTransactionIsolation(int level) throws SQLException
    {
        connection_.setTransactionIsolation(level);
        transactionIsoloation_ = new Integer(level);
    }

    private Boolean autoCommit_;
    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException
    {
        connection_.setAutoCommit(autoCommit);
        autoCommit_ = new Boolean(autoCommit);
    }

    private Boolean readOnly_;
    @Override
    public void setReadOnly(boolean readOnly) throws SQLException
    {
        connection_.setReadOnly(readOnly);
        readOnly_ = new Boolean(readOnly);
    }

    private String catalog_;
    @Override
    public void setCatalog(String catalog) throws SQLException
    {
        connection_.setCatalog(catalog);
        catalog_ = catalog;
    }

    private Map<String, Class<?>> typeMap_;
    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException
    {
        connection_.setTypeMap(map);
        typeMap_ = map;
    }

    private void setAllParameters() throws SQLException
    {
        logger.debug("Resetting all connection parameters");
        if (holdability_ != null)
        {
            connection_.setHoldability(holdability_.intValue());
        }

        if (transactionIsoloation_ != null)
        {
            connection_.setTransactionIsolation(transactionIsoloation_.intValue());
        }

        if (autoCommit_ != null)
        {
            connection_.setAutoCommit(autoCommit_.booleanValue());
        }

        if (readOnly_ != null)
        {
            connection_.setReadOnly(readOnly_.booleanValue());
        }

        if (catalog_ != null)
        {
            connection_.setCatalog(catalog_);
        }

        if (typeMap_ != null)
        {
            connection_.setTypeMap(typeMap_);
        }
    }

    private void connect() throws ClassNotFoundException, SQLException
    {
        try
        {
            Class.forName(driver_);
        }
        catch (ClassNotFoundException e)
        {
            logger.error("The driver for your sql connection was not found.  I tried to load " + driver_);
            throw e;
        }
        DriverManager.setLoginTimeout(5);
        Properties props = new Properties();
        props.setProperty("user", userName_);
        props.setProperty("password", password_);
        if (useUTF8_)
        {
            setUTFCharsetForDB(props, server_);
        }

        connection_ = DriverManager.getConnection(server_, props);
    }

    protected void reconnect() throws SQLException
    {
        logger.debug("Reconnect called on SQL connection");
        int failCount = 0;
        while (true)
        {
            try
            {
                // try to clean up, but don't fail if we can't...
                if (connection_ != null)
                {
                    connection_.close();
                    connection_ = null;
                }
            }
            catch (SQLException e1)
            {
            }

            try
            {
                Properties props = new Properties();
                props.setProperty("user", userName_);
                props.setProperty("password", password_);
                if (useUTF8_)
                {
                    setUTFCharsetForDB(props, server_);
                }
                connection_ = DriverManager.getConnection(server_, props);
                setAllParameters();
                break;

            }
            catch (SQLException e)
            {
                logger.warn("Reconnect failed on attempt " + failCount);
                failCount++;
                if (failCount > maxFailCount_)
                {
                    throw e;
                }
            }
        }
    }

    private static void setUTFCharsetForDB(Properties props, String URL)
    {
        String tempURL = URL.toLowerCase();
        // access and postgres use this flag
        if (tempURL.indexOf("odbc") != -1 || tempURL.indexOf("postgresql") != -1)
        {
            props.setProperty("charSet", "utf-8");
        }
        // mysql uses this
        else if (tempURL.indexOf("mysql") != -1)
        {
            props.setProperty("characterEncoding", "UTF-8");
            props.setProperty("useUnicode", "true");
        }
        else
        {
            props.setProperty("charSet", "utf-8");
        }
    }

    @Override
    public Savepoint setSavepoint() throws SQLException
    {
        throw new java.lang.UnsupportedOperationException("Method setSavepoint not yet implemented.");
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException
    {
        throw new java.lang.UnsupportedOperationException("Method releaseSavepoint not yet implemented.");
    }

    @Override
    public Statement createStatement() throws SQLException
    {
        throw new java.lang.UnsupportedOperationException("Method createStatement not yet implemented.");
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException
    {
        throw new java.lang.UnsupportedOperationException("Method createStatement not yet implemented.");
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException
    {
        throw new java.lang.UnsupportedOperationException("Method createStatement not yet implemented.");
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException
    {
        throw new java.lang.UnsupportedOperationException("Method getTypeMap not yet implemented.");
    }

    public String nativeSQL(String sql) throws SQLException
    {
        throw new java.lang.UnsupportedOperationException("Method nativeSQL not yet implemented.");
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException
    {
        throw new java.lang.UnsupportedOperationException("Method prepareCall not yet implemented.");
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException
    {
        throw new java.lang.UnsupportedOperationException("Method prepareCall not yet implemented.");
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
            int resultSetHoldability) throws SQLException
    {
        throw new java.lang.UnsupportedOperationException("Method prepareCall not yet implemented.");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException
    {
        throw new java.lang.UnsupportedOperationException("Method prepareStatement not yet implemented.");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
            int resultSetHoldability) throws SQLException
    {
        throw new java.lang.UnsupportedOperationException("Method prepareStatement not yet implemented.");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException
    {
        throw new java.lang.UnsupportedOperationException("Method prepareStatement not yet implemented.");
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException
    {
        throw new java.lang.UnsupportedOperationException("Method setSavepoint not yet implemented.");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException
    {
        throw new java.lang.UnsupportedOperationException("Method prepareStatement not yet implemented.");
    }
    
    //New stuff in 1.6

	@Override
	public Array createArrayOf(String arg0, Object[] arg1) throws SQLException
	{
		throw new java.lang.UnsupportedOperationException("Method createArrayOf not yet implemented.");
	}

	@Override
	public Blob createBlob() throws SQLException
	{
		throw new java.lang.UnsupportedOperationException("Method createBlob not yet implemented.");
	}

	@Override
	public Clob createClob() throws SQLException
	{
		throw new java.lang.UnsupportedOperationException("Method createClob not yet implemented.");
	}

	@Override
	public NClob createNClob() throws SQLException
	{
		throw new java.lang.UnsupportedOperationException("Method createNClob not yet implemented.");
	}

	@Override
	public SQLXML createSQLXML() throws SQLException
	{
		throw new java.lang.UnsupportedOperationException("Method createSQLXML not yet implemented.");
	}

	@Override
	public Struct createStruct(String arg0, Object[] arg1) throws SQLException
	{
		throw new java.lang.UnsupportedOperationException("Method createStruct not yet implemented.");
	}

	@Override
	public Properties getClientInfo() throws SQLException
	{
		return connection_.getClientInfo();
	}

	@Override
	public String getClientInfo(String arg0) throws SQLException
	{
		return getClientInfo(arg0);
	}

	@Override
	public boolean isValid(int arg0) throws SQLException
	{
		return connection_.isValid(arg0);
	}

	@Override
	public void setClientInfo(Properties arg0) throws SQLClientInfoException
	{
		throw new java.lang.UnsupportedOperationException("Method setClientInfo not yet implemented.");
	}

	@Override
	public void setClientInfo(String arg0, String arg1) throws SQLClientInfoException
	{
		throw new java.lang.UnsupportedOperationException("Method setClientInfo not yet implemented.");
	}


	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException
	{
		return connection_.isWrapperFor(iface);
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException
	{
		return connection_.unwrap(iface);
	}
	
	public static void main(String[] args) throws ClassNotFoundException, SQLException
	{
		System.out.println("Wrapped Connection log test");
		new WrappedConnection("a", "b", "c", "d");
	}
}