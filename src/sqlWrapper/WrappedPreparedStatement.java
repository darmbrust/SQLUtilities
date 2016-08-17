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

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Enumeration;
import java.util.Hashtable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * An automatically reconnecting prepared statement.  See description in 
 * the WrappedConnection class.
 * 
 * You should get this code from:
 * http://code.google.com/p/armbrust-file-utils/source/browse/#svn%2Ftrunk%2FSQLWrapper-1.6
 * 
 * @author <A HREF="mailto:daniel.armbrust@gmail.com">Dan Armbrust</A>
 */
public class WrappedPreparedStatement implements PreparedStatement
{
    private PreparedStatement                   statement_;
    private WrappedConnection                   wrappedConnection_;

    private Hashtable<Integer, QueryParameter>  setVariables_;
    private String                              sql_;
    private Integer                             fetchDirection_, fetchSize_, maxFieldSize_, maxRows_, queryTimeout_;

    private Integer                             resultSetType_, resultSetConcurrency_;

    private Log logger        = LogFactory.getLog("sqlWrapper.WrappedPreparedStatement");

    public WrappedPreparedStatement(WrappedConnection connection, String sql) throws SQLException
    {
        sql_ = sql;
        setVariables_ = new Hashtable<Integer, QueryParameter>();
        wrappedConnection_ = connection;
        statement_ = wrappedConnection_.connection_.prepareStatement(sql_);
    }

    public WrappedPreparedStatement(WrappedConnection connection, String sql, int resultSetType,
            int resultSetConcurrency) throws SQLException
    {
        sql_ = sql;
        resultSetType_ = new Integer(resultSetType);
        resultSetConcurrency_ = new Integer(resultSetConcurrency);
        setVariables_ = new Hashtable<Integer, QueryParameter>();
        wrappedConnection_ = connection;
        statement_ = wrappedConnection_.connection_.prepareStatement(sql_, resultSetType, resultSetConcurrency);
    }

	@Override
    public void setString(int parameterIndex, String x) throws SQLException
    {
        QueryParameter temp = new QueryParameter(WrapperConstants.STRING, x);
        setType(parameterIndex, temp);
        setVariables_.put(parameterIndex, temp);
    }

	@Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException
    {
        QueryParameter temp = new QueryParameter(WrapperConstants.BOOLEAN, new Boolean(x));
        setType(parameterIndex, temp);
        setVariables_.put(parameterIndex, temp);
    }

	@Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException
    {
        QueryParameter temp = new QueryParameter(WrapperConstants.NULL, new Integer(sqlType));
        setType(parameterIndex, temp);
        setVariables_.put(parameterIndex, temp);
    }

	@Override
    public void setTime(int parameterIndex, Time x) throws SQLException
    {
        QueryParameter temp = new QueryParameter(WrapperConstants.TIME, x);
        setType(parameterIndex, temp);
        setVariables_.put(parameterIndex, temp);
    }

	@Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException
    {
        QueryParameter temp = new QueryParameter(WrapperConstants.TIMESTAMP, x);
        setType(parameterIndex, temp);
        setVariables_.put(parameterIndex, temp);
    }

	@Override
    public void setByte(int parameterIndex, byte x) throws SQLException
    {
        QueryParameter temp = new QueryParameter(WrapperConstants.BYTE, new Byte(x));
        setType(parameterIndex, temp);
        setVariables_.put(parameterIndex, temp);
    }

	@Override
    public void setDouble(int parameterIndex, double x) throws SQLException
    {
        QueryParameter temp = new QueryParameter(WrapperConstants.DOUBLE, new Double(x));
        setType(parameterIndex, temp);
        setVariables_.put(parameterIndex, temp);
    }

	@Override
    public void setFloat(int parameterIndex, float x) throws SQLException
    {
        QueryParameter temp = new QueryParameter(WrapperConstants.FLOAT, new Float(x));
        setType(parameterIndex, temp);
        setVariables_.put(parameterIndex, temp);
    }

	@Override
    public void setInt(int parameterIndex, int x) throws SQLException
    {
        QueryParameter temp = new QueryParameter(WrapperConstants.INT, new Integer(x));
        setType(parameterIndex, temp);
        setVariables_.put(parameterIndex, temp);
    }

	@Override
    public void setLong(int parameterIndex, long x) throws SQLException
    {
        QueryParameter temp = new QueryParameter(WrapperConstants.LONG, new Long(x));
        setType(parameterIndex, temp);
        setVariables_.put(parameterIndex, temp);
    }

	@Override
    public void setShort(int parameterIndex, short x) throws SQLException
    {
        QueryParameter temp = new QueryParameter(WrapperConstants.SHORT, new Short(x));
        setType(parameterIndex, temp);
        setVariables_.put(parameterIndex, temp);
    }

	@Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException
    {
        Byte[] temp1 = new Byte[x.length];
        for (int i = 0; i < temp1.length; i++)
        {
            temp1[i] = new Byte(x[i]);
        }
        QueryParameter temp = new QueryParameter(WrapperConstants.BYTES, temp1);
        setType(parameterIndex, temp);
        setVariables_.put(parameterIndex, temp);
    }

	@Override
    public void setObject(int parameterIndex, Object x) throws SQLException
    {
        QueryParameter temp = new QueryParameter(WrapperConstants.OBJECT, x);
        setType(parameterIndex, temp);
        setVariables_.put(parameterIndex, temp);
    }

	@Override  
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException
    {
        QueryParameter temp = new QueryParameter(WrapperConstants.OBJECT, x, targetSqlType);
        setType(parameterIndex, temp);
        setVariables_.put(parameterIndex, temp);
    }

	@Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException
    {
        QueryParameter temp = new QueryParameter(WrapperConstants.BIGDECIMAL, x);
        setType(parameterIndex, temp);
        setVariables_.put(parameterIndex, temp);
    }

	@Override
    public void setURL(int parameterIndex, URL x) throws SQLException
    {
        QueryParameter temp = new QueryParameter(WrapperConstants.URL, x);
        setType(parameterIndex, temp);
        setVariables_.put(parameterIndex, temp);
    }

	@Override
    public void setArray(int parameterIndex, Array x) throws SQLException
    {
        QueryParameter temp = new QueryParameter(WrapperConstants.ARRAY, x);
        setType(parameterIndex, temp);
        setVariables_.put(parameterIndex, temp);
    }

	@Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException
    {
        QueryParameter temp = new QueryParameter(WrapperConstants.BLOB, x);
        setType(parameterIndex, temp);
        setVariables_.put(parameterIndex, temp);
    }

	@Override
    public void setClob(int parameterIndex, Clob x) throws SQLException
    {
        QueryParameter temp = new QueryParameter(WrapperConstants.CLOB, x);
        setType(parameterIndex, temp);
        setVariables_.put(parameterIndex, temp);
    }

	@Override
    public void setDate(int parameterIndex, Date x) throws SQLException
    {
        QueryParameter temp = new QueryParameter(WrapperConstants.DATE, x);
        setType(parameterIndex, temp);
        setVariables_.put(parameterIndex, temp);
    }

	@Override
    public void setRef(int parameterIndex, Ref x) throws SQLException
    {
        QueryParameter temp = new QueryParameter(WrapperConstants.REF, x);
        setType(parameterIndex, temp);
        setVariables_.put(parameterIndex, temp);
    }

    private void setType(int parameterIndex, QueryParameter value) throws SQLException
    {
        switch (value.type)
        {
            case WrapperConstants.STRING : {
                statement_.setString(parameterIndex, (String) value.value);
                break;
            }
            case WrapperConstants.BOOLEAN : {
                statement_.setBoolean(parameterIndex, ((Boolean) value.value).booleanValue());
                break;
            }
            case WrapperConstants.NULL : {
                statement_.setNull(parameterIndex, ((Integer) value.value).intValue());
                break;
            }
            case WrapperConstants.TIME : {
                statement_.setTime(parameterIndex, ((Time) value.value));
                break;
            }

            case WrapperConstants.TIMESTAMP : {
                statement_.setTimestamp(parameterIndex, ((Timestamp) value.value));
                break;
            }
            case WrapperConstants.BYTE : {
                statement_.setByte(parameterIndex, ((Byte) value.value).byteValue());
                break;
            }
            case WrapperConstants.DOUBLE : {
                statement_.setDouble(parameterIndex, ((Double) value.value).doubleValue());
                break;
            }
            case WrapperConstants.FLOAT : {
                statement_.setFloat(parameterIndex, ((Float) value.value).floatValue());
                break;
            }
            case WrapperConstants.INT : {
                statement_.setInt(parameterIndex, ((Integer) value.value).intValue());
                break;
            }
            case WrapperConstants.LONG : {
                statement_.setLong(parameterIndex, ((Long) value.value).longValue());
                break;
            }
            case WrapperConstants.SHORT : {
                statement_.setShort(parameterIndex, ((Short) value.value).shortValue());
                break;
            }
            case WrapperConstants.BYTES : {
                Byte[] temp = ((Byte[]) value.value);
                byte[] temp1 = new byte[temp.length];
                for (int i = 0; i < temp1.length; i++)
                {
                    temp1[i] = temp[i].byteValue();
                }
                statement_.setBytes(parameterIndex, temp1);
                break;
            }
            case WrapperConstants.OBJECT : {
                if (value.targetType != Integer.MIN_VALUE)
                {
                    statement_.setObject(parameterIndex, value.value, value.targetType);
                }
                else
                {
                    statement_.setObject(parameterIndex, value.value);
                }
                break;
            }
            case WrapperConstants.BIGDECIMAL : {
                statement_.setBigDecimal(parameterIndex, (BigDecimal) value.value);
                break;
            }
            case WrapperConstants.URL : {
                statement_.setURL(parameterIndex, (URL) value.value);
                break;
            }
            case WrapperConstants.ARRAY : {
                statement_.setArray(parameterIndex, (Array) value.value);
                break;
            }
            case WrapperConstants.BLOB : {
                statement_.setBlob(parameterIndex, (Blob) value.value);
                break;
            }
            case WrapperConstants.CLOB : {
                statement_.setClob(parameterIndex, (Clob) value.value);
                break;
            }
            case WrapperConstants.DATE : {
                statement_.setDate(parameterIndex, (Date) value.value);
                break;
            }
            case WrapperConstants.REF : {
                statement_.setRef(parameterIndex, (Ref) value.value);
                break;
            }
            default : {
                throw new SQLException("Unknown object type passed through WrappedPreparedStatment");

            }
        }
    }

    public void close() throws SQLException
    {
        statement_.close();
    }

	@Override
    public void setFetchDirection(int direction) throws SQLException
    {
        statement_.setFetchDirection(direction);
        fetchDirection_ = new Integer(direction);
    }

	@Override
    public void setFetchSize(int rows) throws SQLException
    {
        statement_.setFetchSize(rows);
        fetchSize_ = new Integer(rows);
    }

	@Override
    public void setMaxFieldSize(int max) throws SQLException
    {
        statement_.setMaxFieldSize(max);
        maxFieldSize_ = new Integer(max);

    }

	@Override
    public void setMaxRows(int max) throws SQLException
    {
        statement_.setMaxRows(max);
        maxRows_ = new Integer(max);

    }

	@Override
    public void setQueryTimeout(int seconds) throws SQLException
    {
        statement_.setQueryTimeout(seconds);
        queryTimeout_ = new Integer(seconds);
    }

    Boolean escapeProcessing_;
	@Override
    public void setEscapeProcessing(boolean enable) throws SQLException
    {
        statement_.setEscapeProcessing(enable);
        escapeProcessing_ = new Boolean(enable);
    }

	@Override
    public Connection getConnection() throws SQLException
    {
        return (Connection) wrappedConnection_;
    }

	@Override
    public ResultSet getGeneratedKeys() throws SQLException
    {
        return statement_.getGeneratedKeys();
    }

	@Override
    public ResultSet getResultSet() throws SQLException
    {
        return statement_.getResultSet();
    }

	@Override
    public SQLWarning getWarnings() throws SQLException
    {
        return statement_.getWarnings();
    }

	@Override
    public int getResultSetConcurrency() throws SQLException
    {
        return statement_.getResultSetConcurrency();
    }

	@Override
    public int getResultSetHoldability() throws SQLException
    {
        return statement_.getResultSetHoldability();
    }

	@Override
    public int getResultSetType() throws SQLException
    {
        return statement_.getResultSetType();
    }

	@Override
    public int getFetchDirection() throws SQLException
    {
        return statement_.getFetchDirection();
    }

	@Override
    public int getFetchSize() throws SQLException
    {
        return statement_.getFetchSize();
    }

	@Override
    public int getMaxFieldSize() throws SQLException
    {
        return statement_.getMaxFieldSize();
    }

	@Override
    public int getMaxRows() throws SQLException
    {
        return statement_.getMaxRows();
    }

	@Override
    public int getQueryTimeout() throws SQLException
    {
        return statement_.getQueryTimeout();
    }

	@Override
    public int getUpdateCount() throws SQLException
    {
        return statement_.getUpdateCount();
    }

	@Override
    public void cancel() throws SQLException
    {
        statement_.cancel();
    }

	@Override
    public void clearBatch() throws SQLException
    {
        statement_.clearBatch();
    }

	@Override
    public void clearWarnings() throws SQLException
    {
        statement_.clearWarnings();
    }

	@Override
    public boolean getMoreResults() throws SQLException
    {
        return statement_.getMoreResults();
    }

	@Override
    public void clearParameters() throws SQLException
    {
        setVariables_.clear();
        statement_.clearParameters();
    }

	@Override
    public ResultSetMetaData getMetaData() throws SQLException
    {
        return statement_.getMetaData();
    }

	@Override
    public boolean getMoreResults(int current) throws SQLException
    {
        return statement_.getMoreResults(current);
    }

    private void setAllParameters() throws SQLException
    {
        logger.debug("Resetting all prepared statement parameters");
        if (fetchDirection_ != null)
        {
            statement_.setFetchDirection(fetchDirection_.intValue());
        }
        if (fetchSize_ != null)
        {
            statement_.setFetchSize(fetchSize_.intValue());
        }
        if (maxFieldSize_ != null)
        {
            statement_.setMaxFieldSize(maxFieldSize_.intValue());
        }
        if (maxRows_ != null)
        {
            statement_.setMaxRows(maxRows_.intValue());
        }
        if (queryTimeout_ != null)
        {
            statement_.setQueryTimeout(queryTimeout_.intValue());
        }
        if (escapeProcessing_ != null)
        {
            statement_.setEscapeProcessing(escapeProcessing_.booleanValue());
        }
    }

    private void setAllVariables() throws SQLException
    {
        logger.debug("Resetting all prepared statement variable values");
        Enumeration<Integer> enumerator = setVariables_.keys();
        while (enumerator.hasMoreElements())
        {
            Integer index = enumerator.nextElement();
            setType(index.intValue(), setVariables_.get(index));
        }
    }

    private void rebuildStatement() throws SQLException
    {
        logger.debug("recreating the prepared statement");
        if (resultSetConcurrency_ != null && resultSetType_ != null)
        {
            statement_ = wrappedConnection_.connection_.prepareStatement(sql_, resultSetType_.intValue(),
                                                                         resultSetConcurrency_.intValue());
        }
        else
        {
            statement_ = wrappedConnection_.connection_.prepareStatement(sql_);
        }
    }

    private void rebuildAll() throws SQLException
    {
        boolean recreatedConnection = false;
        boolean isClosed = false;
        
        try
        {
            isClosed = wrappedConnection_.isClosed();
        }
        catch (SQLException e)
        {
            isClosed = true;
        }
        if (isClosed)
        {
            wrappedConnection_.reconnect();
            recreatedConnection = true;
        }

        try
        {
            // clean up resources...
            statement_.close();
            statement_ = null;
        }
        catch (SQLException e1)
        {
        }

        // try the query again.

        try
        {
            rebuildStatement();
        }
        catch (SQLException e)
        {
            //if we couldn't rebuild the statement, and we didn't recreate the 
            //connection, recreate the connection
            if (!recreatedConnection)
            {
                wrappedConnection_.reconnect();
                rebuildStatement();
            }
            else
            {
                throw e;
            }
        }
        setAllVariables();
        setAllParameters();
    }

    private String toString(String sql, boolean throwException) throws SQLException
    {
        if (sql == null)
        {
            sql = "";
        }
        StringBuilder temp = new StringBuilder("WrappedPreparedStatement - query: \"" + sql + "\"");
        int parameterIndex = 1;
        for (int i = 0; i < temp.length(); i++)
        {
            if (temp.charAt(i) == '?')
            {
                QueryParameter para = ((QueryParameter) setVariables_.get(new Integer(parameterIndex++)));
                if (para == null)
                {
                    if (throwException)
                    {
                        throw new SQLException("You forgot to set parameter " + parameterIndex);
                    }
                    else
                    {
                        para = new QueryParameter(0, "--UNSET_PARAMETER--");
                    }
                }
                
                String replacementValue = "";
                if (para.type == WrapperConstants.NULL)
                {
                    replacementValue = "'null'";
                }
                else
                {
                    replacementValue = "'" + (para.value == null ? "null" : para.value.toString()) + "'";
                }
                temp.replace(i, i + 1, replacementValue);
                i = i + replacementValue.length();
            }
        }
        return temp.toString();
    }

	@Override
    public String toString()
    {
        try
        {
            return toString(this.sql_, false);
        }
        catch (SQLException e)
        {
            // this exception actually won't be thrown because of the above false param.
            return null;
        }
    }

    private void debugQuery(String sql) throws SQLException
    {
        if (logger.isDebugEnabled())
        {
            logger.debug("Executing query: " + this.toString(sql, true));
        }
    }

	@Override
    public ResultSet executeQuery() throws SQLException
    {
        debugQuery(sql_);
        try
        {
            return statement_.executeQuery();
        }
        catch (Exception e)
        {
            // try the query again.
            try
            {
                rebuildAll();
                return statement_.executeQuery();
            }
            catch (SQLException e1)
            {
                // if anything goes wrong in retrying the query, lets just throw the origional exception
                if (e instanceof SQLException)
                {
                    throw (SQLException) e;
                }
                else
                {
                    throw new SQLException("Unexpected Error " + e.toString());
                }
            }
        }
    }

	@Override
    public ResultSet executeQuery(String sql) throws SQLException
    {
        debugQuery(sql);
        try
        {
            return statement_.executeQuery(sql);
        }
        catch (Exception e)
        {
            // try the query again.
            try
            {
                rebuildAll();
                return statement_.executeQuery(sql);
            }
            catch (SQLException e1)
            {
                // if anything goes wrong in retrying the query, lets just throw the origional exception
                if (e instanceof SQLException)
                {
                    throw (SQLException) e;
                }
                else
                {
                    throw new SQLException("Unexpected Error " + e.toString());
                }
            }

        }
    }

	@Override
    public boolean execute(String sql) throws SQLException
    {
        debugQuery(sql);
        try
        {
            return statement_.execute(sql);
        }
        catch (SQLException e)
        {
            // try the query again.
            try
            {
                rebuildAll();
                return statement_.execute(sql);
            }
            catch (SQLException e1)
            {
                // if anything goes wrong in retrying the query, lets just throw the original exception
                if (e instanceof SQLException)
                {
                    throw (SQLException) e;
                }
                else
                {
                    throw new SQLException("Unexpected Error " + e.toString());
                }
            }

        }
    }

	@Override
    public int executeUpdate() throws SQLException
    {
        debugQuery(sql_);

        try
        {
            return statement_.executeUpdate();
        }
        catch (SQLException e)
        {
            // try the query again.
            try
            {
                rebuildAll();
                return statement_.executeUpdate();
            }
            catch (SQLException e1)
            {
                // if anything goes wrong in retrying the query, lets just throw the original exception
                if (e instanceof SQLException)
                {
                    throw (SQLException) e;
                }
                else
                {
                    throw new SQLException("Unexpected Error " + e.toString());
                }
            }

        }
    }

	@Override
    public int executeUpdate(String sql) throws SQLException
    {
        debugQuery(sql);
        try
        {
            return statement_.executeUpdate(sql);
        }
        catch (SQLException e)
        {
            // try the query again.
            try
            {
                rebuildAll();
                return statement_.executeUpdate(sql);
            }
            catch (SQLException e1)
            {
                // if anything goes wrong in retrying the query, lets just throw the original exception
                if (e instanceof SQLException)
                {
                    throw (SQLException) e;
                }
                else
                {
                    throw new SQLException("Unexpected Error " + e.toString());
                }
            }

        }
    }

    private class QueryParameter
    {
        int    type;
        Object value;
        int targetType;

        public QueryParameter(int type, Object value)
        {
            this.type = type;
            this.value = value;
            this.targetType = Integer.MIN_VALUE;
        }
        
        public QueryParameter(int type, Object value, int targetSqlType)
        {
            this.type = type;
            this.value = value;
            this.targetType = targetSqlType;
        }
    }

	@Override
    public void addBatch() throws SQLException
    {
        throw new java.lang.UnsupportedOperationException("Method addBatch not yet implemented.");
    }

	@Override
    public boolean execute() throws SQLException
    {
        debugQuery(sql_);
        try
        {
            return statement_.execute();
        }
        catch (SQLException e)
        {
            // try the query again.
            try
            {
                rebuildAll();
                return statement_.execute();
            }
            catch (SQLException e1)
            {
                // if anything goes wrong in retrying the query, lets just throw the original exception
                if (e instanceof SQLException)
                {
                    throw (SQLException) e;
                }
                else
                {
                    throw new SQLException("Unexpected Error " + e.toString());
                }
            }

        }
    }

	@Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException
    {
        throw new java.lang.UnsupportedOperationException("Method setAsciiStream not yet implemented.");
    }

	@Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException
    {
        throw new java.lang.UnsupportedOperationException("Method setBinaryStream not yet implemented.");
    }

    /** @deprecated */

	@Override
	public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException
    {
        throw new java.lang.UnsupportedOperationException("Method setUnicodeStream not yet implemented.");
    }

	@Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException
    {
        throw new java.lang.UnsupportedOperationException("Method setCharacterStream not yet implemented.");
    }

	@Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scale) throws SQLException
    {
        throw new java.lang.UnsupportedOperationException("Method setObject not yet implemented.");
    }

	@Override
    public void setNull(int paramIndex, int sqlType, String typeName) throws SQLException
    {
        throw new java.lang.UnsupportedOperationException("Method setNull not yet implemented.");
    }

	@Override
    public ParameterMetaData getParameterMetaData() throws SQLException
    {
        throw new java.lang.UnsupportedOperationException("Method getParameterMetaData not yet implemented.");
    }

	@Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException
    {
        throw new java.lang.UnsupportedOperationException("Method setDate not yet implemented.");
    }

	@Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException
    {
        throw new java.lang.UnsupportedOperationException("Method setTime not yet implemented.");
    }

	@Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException
    {
        throw new java.lang.UnsupportedOperationException("Method setTimestamp not yet implemented.");
    }

	@Override
    public int[] executeBatch() throws SQLException
    {
        throw new java.lang.UnsupportedOperationException("Method executeBatch not yet implemented.");
    }

	@Override
    public void addBatch(String sql) throws SQLException
    {
        throw new java.lang.UnsupportedOperationException("Method addBatch not yet implemented.");
    }

	@Override
    public void setCursorName(String name) throws SQLException
    {
        throw new java.lang.UnsupportedOperationException("Method setCursorName not yet implemented.");
    }

	@Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException
    {
        throw new java.lang.UnsupportedOperationException("Method executeUpdate not yet implemented.");
    }

	@Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException
    {
        throw new java.lang.UnsupportedOperationException("Method execute not yet implemented.");
    }

	@Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException
    {
        throw new java.lang.UnsupportedOperationException("Method executeUpdate not yet implemented.");
    }

	@Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException
    {
        throw new java.lang.UnsupportedOperationException("Method execute not yet implemented.");
    }

	@Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException
    {
        throw new java.lang.UnsupportedOperationException("Method executeUpdate not yet implemented.");
    }

	@Override
    public boolean execute(String sql, String[] columnNames) throws SQLException
    {
        throw new java.lang.UnsupportedOperationException("Method execute not yet implemented.");
    }

	//new in 1.6
	
	@Override
	public void setAsciiStream(int arg0, InputStream arg1) throws SQLException
	{
		throw new java.lang.UnsupportedOperationException("Method setAsciiStream not yet implemented.");
	}

	@Override
	public void setAsciiStream(int arg0, InputStream arg1, long arg2) throws SQLException
	{
		throw new java.lang.UnsupportedOperationException("Method setAsciiStream not yet implemented.");
	}

	@Override
	public void setBinaryStream(int arg0, InputStream arg1) throws SQLException
	{
		throw new java.lang.UnsupportedOperationException("Method setBinaryStream not yet implemented.");
	}

	@Override
	public void setBinaryStream(int arg0, InputStream arg1, long arg2) throws SQLException
	{
		throw new java.lang.UnsupportedOperationException("Method setBinaryStream not yet implemented.");
	}

	@Override
	public void setBlob(int arg0, InputStream arg1) throws SQLException
	{
		throw new java.lang.UnsupportedOperationException("Method setBlob not yet implemented.");
	}

	@Override
	public void setBlob(int arg0, InputStream arg1, long arg2) throws SQLException
	{
		throw new java.lang.UnsupportedOperationException("Method setBlob not yet implemented.");
	}

	@Override
	public void setCharacterStream(int arg0, Reader arg1) throws SQLException
	{
		throw new java.lang.UnsupportedOperationException("Method setCharacterStream not yet implemented.");
	}

	@Override
	public void setCharacterStream(int arg0, Reader arg1, long arg2) throws SQLException
	{
		throw new java.lang.UnsupportedOperationException("Method setCharacterStream not yet implemented.");
	}

	@Override
	public void setClob(int arg0, Reader arg1) throws SQLException
	{
		throw new java.lang.UnsupportedOperationException("Method setClob not yet implemented.");
	}

	@Override
	public void setClob(int arg0, Reader arg1, long arg2) throws SQLException
	{
		throw new java.lang.UnsupportedOperationException("Method setClob not yet implemented.");
	}

	@Override
	public void setNCharacterStream(int arg0, Reader arg1) throws SQLException
	{
		throw new java.lang.UnsupportedOperationException("Method setNCharacterStream not yet implemented.");
	}

	@Override
	public void setNCharacterStream(int arg0, Reader arg1, long arg2) throws SQLException
	{
		throw new java.lang.UnsupportedOperationException("Method setNCharacterStream not yet implemented.");
	}

	@Override
	public void setNClob(int arg0, NClob arg1) throws SQLException
	{
		throw new java.lang.UnsupportedOperationException("Method setNClob not yet implemented.");
	}

	@Override
	public void setNClob(int arg0, Reader arg1) throws SQLException
	{
		throw new java.lang.UnsupportedOperationException("Method setNClob not yet implemented.");
	}

	@Override
	public void setNClob(int arg0, Reader arg1, long arg2) throws SQLException
	{
		throw new java.lang.UnsupportedOperationException("Method setNClob not yet implemented.");
	}

	@Override
	public void setNString(int arg0, String arg1) throws SQLException
	{
		throw new java.lang.UnsupportedOperationException("Method setNString not yet implemented.");
	}

	@Override
	public void setRowId(int arg0, RowId arg1) throws SQLException
	{
		throw new java.lang.UnsupportedOperationException("Method setRowId not yet implemented.");
	}

	@Override
	public void setSQLXML(int arg0, SQLXML arg1) throws SQLException
	{
		throw new java.lang.UnsupportedOperationException("Method setSQLXML not yet implemented.");
	}

	@Override
	public boolean isClosed() throws SQLException
	{
		return statement_.isClosed();
	}

	@Override
	public boolean isPoolable() throws SQLException
	{
		return statement_.isPoolable();
	}

	@Override
	public void setPoolable(boolean arg0) throws SQLException
	{
		throw new java.lang.UnsupportedOperationException("Method setPoolable not yet implemented.");
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException
	{
		return statement_.isWrapperFor(iface);
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException
	{
		return statement_.unwrap(iface);
	}
}