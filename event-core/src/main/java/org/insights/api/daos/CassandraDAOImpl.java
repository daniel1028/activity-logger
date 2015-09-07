package org.insights.api.daos;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;

import org.insights.api.constants.Constants;
import org.insights.api.model.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.ExceptionCallback;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.query.IndexQuery;
import com.netflix.astyanax.retry.ConstantBackoff;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.util.RangeBuilder;
@Repository
public class CassandraDAOImpl extends CassandraConnectionProvider implements CassandraDAO,Constants {

	private static final Logger logger = LoggerFactory.getLogger(CassandraDAOImpl.class);

	private static final SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd kk:mm:ss");

	private static final ConsistencyLevel DEFAULT_CONSISTENCY_LEVEL = ConsistencyLevel.CL_QUORUM;
	
	/**
	 * This method using to read data with single Key&Indexed column.
	 * 
	 * @param cfName
	 * @param key
	 * @param columnName
	 * @param retryCount
	 * @return
	 */
	public Column<String> readWithKeyColumn(String cfName, String key, String columnName, int retryCount) {

		Column<String> result = null;
		ColumnList<String> columnList = null;
		try {
			columnList = getKeyspace().prepareQuery(this.accessColumnFamily(cfName)).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5)).getKey(key).execute()
					.getResult();

		} catch (Exception e) {
			if (e instanceof ConnectionException && retryCount < 6) {
				return readWithKeyColumn(cfName, key, columnName, ++retryCount);
			}
			logger.error("Exception:",e);
		}
		if (columnList != null) {
			result = columnList.getColumnByName(columnName);
		}
		return result;
	}

	/**
	 * This method using to read data with single Key and multiple Indexed columns.
	 * 
	 * @param cfName
	 * @param key
	 * @param columnList
	 * @param retryCount
	 * @return
	 */
	public ColumnList<String> readWithKeyColumnList(String cfName, String key, Collection<String> columnList, int retryCount) {

		ColumnList<String> result = null;
		try {
			result = getKeyspace().prepareQuery(this.accessColumnFamily(cfName)).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5)).getKey(key)
					.withColumnSlice(columnList).execute().getResult();

		} catch (Exception e) {

			if (e instanceof ConnectionException && retryCount < 6) {
				return readWithKeyColumnList(cfName, key, columnList, ++retryCount);
			}
			logger.error("Exception:",e);
		}

		return result;
	}

	/**
	 * This method using to read data with multiple Keys and multiple Indexed columns.
	 * 
	 * @param cfName
	 * @param keys
	 * @param columnList
	 * @param retryCount
	 * @return
	 */
	public Rows<String, String> readWithKeyListColumnList(String cfName, Collection<String> keys, Collection<String> columnList, int retryCount) {

		Rows<String, String> result = null;
		try {
			result = getKeyspace().prepareQuery(this.accessColumnFamily(cfName)).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5)).getKeySlice(keys)
					.withColumnSlice(columnList).execute().getResult();

		} catch (Exception e) {

			if (e instanceof ConnectionException && retryCount < 6) {
				return readWithKeyListColumnList(cfName, keys, columnList, ++retryCount);
			}
			logger.error("Exception:",e);
		}

		return result;
	}

	/**
	 * This method using to read data with single Key.
	 * 
	 * @param cfName
	 * @param key
	 * @param retryCount
	 * @return
	 */
	public ColumnList<String> readWithKey(String cfName, String key, int retryCount) {

		ColumnList<String> result = null;
		try {
			result = getKeyspace().prepareQuery(this.accessColumnFamily(cfName)).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5)).getKey(key).execute()
					.getResult();

		} catch (Exception e) {

			if (e instanceof ConnectionException && retryCount < 6) {
				return readWithKey(cfName, key, ++retryCount);
			}
			logger.info("Error while reading row key : {}",key);
			logger.error("Exception:",e);
		}

		return result;
	}

	/**
	 * This method using to read data with multiple Keys.
	 * 
	 * @param cfName
	 * @param key
	 * @param retryCount
	 * @return
	 */
	public Rows<String, String> readWithKeyList(String cfName, Collection<String> key, int retryCount) {

		Rows<String, String> result = null;
		try {
			result = getKeyspace().prepareQuery(this.accessColumnFamily(cfName)).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5)).getKeySlice(key)
					.execute().getResult();

		} catch (Exception e) {
			if (e instanceof ConnectionException && retryCount < 6) {
				return readWithKeyList(cfName, key, ++retryCount);
			}
			logger.error("Exception:",e);
		}

		return result;
	}

	/**
	 * This method is using to get the data with multiple Keys in comma separated.
	 * 
	 * @param cfName
	 * @param retryCount
	 * @param key
	 * @return
	 */
	public Rows<String, String> readCommaKeyList(String cfName, int retryCount, String... key) {

		Rows<String, String> result = null;
		try {
			result = getKeyspace().prepareQuery(this.accessColumnFamily(cfName)).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5)).getKeySlice(key)
					.execute().getResult();

		} catch (Exception e) {
			if (e instanceof ConnectionException && retryCount < 6) {
				return readCommaKeyList(cfName, ++retryCount, key);
			}
			logger.error("Exception:",e);
		}

		return result;
	}

	/**
	 * 
	 * @param cfName
	 * @param keys
	 * @return
	 */
	public Rows<String, String> readIterableKeyList(String cfName, Iterable<String> keys) {

		Rows<String, String> result = null;
		try {
			result = getKeyspace().prepareQuery(this.accessColumnFamily(cfName)).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5)).getKeySlice(keys)
					.execute().getResult();

		} catch (Exception e) {
			logger.error("Exception:",e);
		}

		return result;
	}

	/**
	 * This method is using to get data with single String data type Indexed column.
	 * 
	 * @param cfName
	 * @param columnName
	 * @param value
	 * @param retryCount
	 * @return
	 */
	public Rows<String, String> readIndexedColumn(String cfName, String columnName, String value, int retryCount) {

		Rows<String, String> result = null;
		try {
			result = getKeyspace().prepareQuery(this.accessColumnFamily(cfName)).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5)).searchWithIndex()
					.addExpression().whereColumn(columnName).equals().value(value).execute().getResult();

		} catch (Exception e) {
			if (e instanceof ConnectionException && retryCount < 6) {
				return readIndexedColumn(cfName, columnName, value, ++retryCount);
			}
			logger.error("Exception:",e);
		}
		return result;
	}

	/**
	 * This method is using to get data with single Long data type Indexed column.
	 * 
	 * @param cfName
	 * @param columnName
	 * @param value
	 * @param retryCount
	 * @return
	 */
	public Rows<String, String> readIndexedColumn(String cfName, String columnName, long value, int retryCount) {

		Rows<String, String> result = null;
		try {
			result = getKeyspace().prepareQuery(this.accessColumnFamily(cfName)).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5)).searchWithIndex()
					.addExpression().whereColumn(columnName).equals().value(value).execute().getResult();

		} catch (Exception e) {
			if (e instanceof ConnectionException && retryCount < 6) {
				return readIndexedColumn(cfName, columnName, value, ++retryCount);
			}
			logger.error("Exception:",e);
		}
		return result;
	}

	/**
	 * This method is using to read rows from bottom.
	 * 
	 * @param cfName
	 * @param columnName
	 * @param value
	 * @param rowsToRead
	 * @param retryCount
	 * @return
	 */
	public Rows<String, String> readIndexedColumnLastNrows(String cfName, String columnName, String value, Integer rowsToRead, int retryCount) {

		Rows<String, String> result = null;
		try {
			result = getKeyspace().prepareQuery(this.accessColumnFamily(cfName)).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5)).searchWithIndex()
					.autoPaginateRows(true).setRowLimit(rowsToRead.intValue()).addExpression().whereColumn(columnName).equals().value(value).execute().getResult();
		} catch (Exception e) {
			if (e instanceof ConnectionException && retryCount < 6) {
				return readIndexedColumnLastNrows(cfName, columnName, value, rowsToRead, ++retryCount);
			}
			logger.error("Exception:",e);
		}

		return result;
	}

	/**
	 * This method is using to read columns which have recently introduced.To use this method, Cassandra has to store columns order by enable NTP config.
	 * 
	 * @param cfName
	 * @param key
	 * @param columnsToRead
	 * @param retryCount
	 * @return
	 */
	public ColumnList<String> readKeyLastNColumns(String cfName, String key, Integer columnsToRead, int retryCount) {

		ColumnList<String> result = null;
		try {
			result = getKeyspace().prepareQuery(this.accessColumnFamily(cfName)).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5)).getKey(key)
					.withColumnRange(new RangeBuilder().setReversed().setLimit(columnsToRead.intValue()).build()).execute().getResult();
		} catch (Exception e) {
			if (e instanceof ConnectionException && retryCount < 6) {
				return readKeyLastNColumns(cfName, key, columnsToRead, retryCount);
			}
			logger.error("Exception:",e);
		}

		return result;
	}

	/**
	 * This method is using to get row count.
	 * 
	 * @param cfName
	 * @param key
	 * @return
	 */
	public long getCount(String cfName, String key) {

		Integer columns = null;

		try {
			columns = getKeyspace().prepareQuery(this.accessColumnFamily(cfName)).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5)).getKey(key).getCount()
					.execute().getResult();
		} catch (Exception e) {
			logger.info("Error while getting column count for - {}",key);
			logger.error("Exception:",e);
		}

		return columns.longValue();
	}

	/**
	 * This method is using to get data with more number of indexed columns.
	 * 
	 * @param cfName
	 * @param columnList
	 * @param retryCount
	 * @return
	 */
	public Rows<String, String> readIndexedColumnList(String cfName, Map<String, String> columnList, int retryCount) {

		Rows<String, String> result = null;
		IndexQuery<String, String> query = null;

		query = getKeyspace().prepareQuery(this.accessColumnFamily(cfName)).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5)).searchWithIndex();

		for (Map.Entry<String, String> entry : columnList.entrySet()) {
			query.addExpression().whereColumn(entry.getKey()).equals().value(entry.getValue());
		}

		try {
			result = query.execute().getResult();
		} catch (Exception e) {
			if (e instanceof ConnectionException && retryCount < 6) {
				return readIndexedColumnList(cfName, columnList, ++retryCount);
			}
			logger.error("Exception:",e);
		}

		return result;
	}

	/**
	 * Just to check if row is available or not for given key.
	 * 
	 * @param cfName
	 * @param key
	 * @param retryCount
	 * @return
	 */
	public boolean isRowKeyExists(String cfName, String key, int retryCount) {

		try {
			return getKeyspace().prepareQuery(this.accessColumnFamily(cfName)).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5)).getKey(key).execute()
					.getResult().isEmpty();
		} catch (Exception e) {
			if (e instanceof ConnectionException && retryCount < 6) {
				return isRowKeyExists(cfName, key, ++retryCount);
			}
			logger.error("Exception:",e);
		}
		return false;
	}

	/**
	 * Just to check if rows available while use indexed columns.
	 * 
	 * @param cfName
	 * @param columns
	 * @return
	 */
	public boolean isValueExists(String cfName, Map<String, Object> columns) {

		try {
			IndexQuery<String, String> cfQuery = getKeyspace().prepareQuery(this.accessColumnFamily(cfName)).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
					.withRetryPolicy(new ConstantBackoff(2000, 5)).searchWithIndex();

			for (Map.Entry<String, Object> map : columns.entrySet()) {
				cfQuery.addExpression().whereColumn(map.getKey()).equals().value(map.getValue().toString());
			}
			return cfQuery.execute().getResult().isEmpty();
		} catch (Exception e) {
			logger.error("Exception:",e);
		}
		return false;
	}

	/**
	 * Get all the Keys for indexed columns.
	 * 
	 * @param cfName
	 * @param columns
	 * @return
	 */
	public Collection<String> getKey(String cfName, Map<String, Object> columns) {

		try {

			IndexQuery<String, String> cfQuery = getKeyspace().prepareQuery(this.accessColumnFamily(cfName)).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
					.withRetryPolicy(new ConstantBackoff(2000, 5)).searchWithIndex();

			for (Map.Entry<String, Object> map : columns.entrySet()) {
				cfQuery.addExpression().whereColumn(map.getKey()).equals().value(map.getValue().toString());
			}
			Rows<String, String> rows = cfQuery.execute().getResult();
			return rows.getKeys();
		} catch (Exception e) {
			logger.error("Exception:",e);
		}
		return null;
	}

	/**
	 * Get data based on column range.
	 * 
	 * @param cfName
	 * @param rowKey
	 * @param startColumnNamePrefix
	 * @param endColumnNamePrefix
	 * @param rowsToRead
	 * @return
	 */
	public ColumnList<String> readColumnsWithPrefix(String cfName, String rowKey, String startColumnNamePrefix, String endColumnNamePrefix, Integer rowsToRead) {
		ColumnList<String> result = null;
		String startColumnPrefix = null;
		String endColumnPrefix = null;
		startColumnPrefix = startColumnNamePrefix + "~\u0000";
		endColumnPrefix = endColumnNamePrefix + "~\uFFFF";

		try {
			result = getKeyspace().prepareQuery(this.accessColumnFamily(cfName)).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5)).getKey(rowKey)
					.withColumnRange(new RangeBuilder().setLimit(rowsToRead).setStart(startColumnPrefix).setEnd(endColumnPrefix).build()).execute().getResult();
		} catch (Exception e) {
			logger.error("Exception:",e);
		}

		return result;
	}

	/**
	 * Get all the rows from ColumnFamily.Please use this method for less number of data.
	 * 
	 * @param cfName
	 * @param retryCount
	 * @return
	 */
	public Rows<String, String> readAllRows(String cfName, int retryCount) {

		Rows<String, String> result = null;
		try {
			result = getKeyspace().prepareQuery(this.accessColumnFamily(cfName)).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5)).getAllRows()
					.withColumnRange(new RangeBuilder().setMaxSize(10).build()).setExceptionCallback(new ExceptionCallback() {
						@Override
						public boolean onException(ConnectionException e) {
							try {
								Thread.sleep(1000);
							} catch (InterruptedException e1) {
							}
							return true;
						}
					}).execute().getResult();
		} catch (Exception e) {
			if (e instanceof ConnectionException && retryCount < 6) {
				return readAllRows(cfName, ++retryCount);
			}
			logger.error("Exception:",e);
		}

		return result;
	}

	/**
	 * Saving multiple String columns & values for single Key.
	 * 
	 * @param cfName
	 * @param key
	 * @param columnValueList
	 */
	public void saveBulkStringList(String cfName, String key, Map<String, String> columnValueList) {

		MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5));

		for (Map.Entry<String, String> entry : columnValueList.entrySet()) {
			m.withRow(this.accessColumnFamily(cfName), key).putColumnIfNotNull(entry.getKey(), entry.getValue(), null);
			;
		}

		try {
			m.execute();
		} catch (Exception e) {
			logger.error("Exception:",e);
		}
	}

	/**
	 * Saving multiple Long columns & values for single Key.
	 * 
	 * @param cfName
	 * @param key
	 * @param columnValueList
	 */
	public void saveBulkLongList(String cfName, String key, Map<String, Long> columnValueList) {

		MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5));

		for (Map.Entry<String, Long> entry : columnValueList.entrySet()) {
			m.withRow(this.accessColumnFamily(cfName), key).putColumnIfNotNull(entry.getKey(), entry.getValue(), null);
			;
		}

		try {
			m.execute();
		} catch (Exception e) {
			logger.error("Exception:",e);
		}
	}

	/**
	 * Saving multiple (String,Integer,Long,Boolean) columns & values for single Key.
	 * 
	 * @param cfName
	 * @param key
	 * @param columnValueList
	 */
	public void saveBulkList(String cfName, String key, Map<String, Object> columnValueList) {

		MutationBatch mutation = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5));

		ColumnListMutation<String> m = mutation.withRow(this.accessColumnFamily(cfName), key);

		for (Entry<String, Object> entry : columnValueList.entrySet()) {
			if (entry.getValue().getClass().getSimpleName().equalsIgnoreCase("String")) {
				m.putColumnIfNotNull(entry.getKey(), String.valueOf(entry.getValue()), null);
			}
			if (entry.getValue().getClass().getSimpleName().equalsIgnoreCase("Integer")) {
				m.putColumnIfNotNull(entry.getKey(), Integer.valueOf(String.valueOf(entry.getValue())));
			}
			if (entry.getValue().getClass().getSimpleName().equalsIgnoreCase("Long")) {
				m.putColumnIfNotNull(entry.getKey(), Long.valueOf(String.valueOf(entry.getValue())));
			}
			if (entry.getValue().getClass().getSimpleName().equalsIgnoreCase("Boolean")) {
				m.putColumnIfNotNull(entry.getKey(), Boolean.valueOf(String.valueOf(entry.getValue())));
			}
			if (entry.getValue().getClass().getSimpleName().equalsIgnoreCase("Date")) {
				m.putColumnIfNotNull(entry.getKey(), (Date) entry.getValue());
			}
		}
		try {
			mutation.execute();
		} catch (Exception e) {
			logger.error("Exception:",e);
		}
	}

	/**
	 * Saving String single column,value for a key.
	 * 
	 * @param cfName
	 * @param key
	 * @param columnName
	 * @param value
	 */
	public void saveStringValue(String cfName, String key, String columnName, String value) {

		MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5));

		m.withRow(this.accessColumnFamily(cfName), key).putColumnIfNotNull(columnName, value, null);

		try {
			m.execute();
		} catch (Exception e) {
			logger.error("Exception:",e);
		}
	}

	/**
	 * Saving String single column,value for a key with expired time so that data will be removed while given time get expire.
	 * 
	 * @param cfName
	 * @param key
	 * @param columnName
	 * @param value
	 * @param expireTime
	 */
	public void saveStringValue(String cfName, String key, String columnName, String value, int expireTime) {

		MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5));

		m.withRow(this.accessColumnFamily(cfName), key).putColumnIfNotNull(columnName, value, expireTime);

		try {
			m.execute();
		} catch (Exception e) {
			logger.error("Exception:",e);
		}
	}

	/**
	 * Saving Long single column,value for a key with expired time so that data will be removed while given time get expire.
	 * 
	 * @param cfName
	 * @param key
	 * @param columnName
	 * @param value
	 * @param expireTime
	 */
	public void saveLongValue(String cfName, String key, String columnName, long value, int expireTime) {

		MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5));

		m.withRow(this.accessColumnFamily(cfName), key).putColumnIfNotNull(columnName, value, expireTime);

		try {
			m.execute();
		} catch (Exception e) {
			logger.error("Exception:",e);
		}
	}

	/**
	 * 
	 * @param cfName
	 * @param key
	 * @param columnName
	 * @param value
	 */
	public void saveLongValue(String cfName, String key, String columnName, long value) {

		MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5));

		m.withRow(this.accessColumnFamily(cfName), key).putColumnIfNotNull(columnName, value, null);

		try {
			m.execute();
		} catch (Exception e) {
			logger.error("Exception:",e);
		}
	}

	/**
	 * Save any type of value.
	 * 
	 * @param cfName
	 * @param key
	 * @param columnName
	 * @param value
	 */
	public void saveValue(String cfName, String key, String columnName, Object value) {

		MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5));
		if (value.getClass().getSimpleName().equalsIgnoreCase("String")) {
			m.withRow(this.accessColumnFamily(cfName), key).putColumnIfNotNull(columnName, String.valueOf(value), null);
		}else if (value.getClass().getSimpleName().equalsIgnoreCase("Integer")) {
			m.withRow(this.accessColumnFamily(cfName), key).putColumnIfNotNull(columnName, Integer.valueOf("" + value), null);
		}else if (value.getClass().getSimpleName().equalsIgnoreCase("Long")) {
			m.withRow(this.accessColumnFamily(cfName), key).putColumnIfNotNull(columnName, Long.valueOf("" + value), null);
		}else if (value.getClass().getSimpleName().equalsIgnoreCase("Boolean")) {
			m.withRow(this.accessColumnFamily(cfName), key).putColumnIfNotNull(columnName, Boolean.valueOf("" + value), null);
		} else {
			m.withRow(this.accessColumnFamily(cfName), key).putColumnIfNotNull(columnName, String.valueOf(value), null);
		}

		try {
			m.execute();
		} catch (Exception e) {
			logger.error("Exception:",e);
		}
	}

	/**
	 * Add values in counter columnfamily.
	 * 
	 * @param cfName
	 * @param key
	 * @param columnName
	 * @param value
	 */
	public void increamentCounter(String cfName, String key, String columnName, long value) {

		MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5));

		m.withRow(this.accessColumnFamily(cfName), key).incrementCounterColumn(columnName, value);

		try {
			m.execute();
		} catch (Exception e) {
			logger.error("Exception:",e);
		}
	}

	/**
	 * Generate Counter mutation batch for counter column family.
	 * 
	 * @param cfName
	 * @param key
	 * @param columnName
	 * @param value
	 * @param m
	 */
	public void generateCounter(String cfName, String key, String columnName, long value, MutationBatch m) {
		m.withRow(this.accessColumnFamily(cfName), key).incrementCounterColumn(columnName, value);
	}

	/**
	 * Generate Mutation batch for any type of data
	 * 
	 * @param columnName
	 * @param value
	 * @param m
	 */
	public void generateNonCounter(String columnName, Object value, ColumnListMutation<String> m) {
		if (value != null) {
			if (value.getClass().getSimpleName().equalsIgnoreCase("String")) {
				m.putColumnIfNotNull(columnName, String.valueOf(value), null);
			}
			if (value.getClass().getSimpleName().equalsIgnoreCase("Integer")) {
				m.putColumnIfNotNull(columnName, Integer.valueOf(String.valueOf(value)));
			}
			if (value.getClass().getSimpleName().equalsIgnoreCase("Long")) {
				m.putColumnIfNotNull(columnName, Long.valueOf(String.valueOf(value)));
			}
			if (value.getClass().getSimpleName().equalsIgnoreCase("Boolean")) {
				m.putColumnIfNotNull(columnName, Boolean.valueOf(String.valueOf(value)));
			}
			if (value.getClass().getSimpleName().equalsIgnoreCase("Timestamp")) {
				try {
					m.putColumnIfNotNull(columnName, new Timestamp((formatter.parse(value.toString())).getTime()));
				} catch (Exception e) {
					e.printStackTrace();
					logger.error("Exception : columnName" + columnName + ": value : " + value);
				}
			}
		} else {
			logger.error("Column Value is null");
		}
	}

	/**
	 * Generate Mutation batch with String value.
	 * 
	 * @param cfName
	 * @param key
	 * @param columnName
	 * @param value
	 * @param m
	 */
	public void generateNonCounter(String cfName, String key, String columnName, String value, MutationBatch m) {
		m.withRow(this.accessColumnFamily(cfName), key).putColumnIfNotNull(columnName, value);
	}

	/**
	 * Generate Mutation batch with Long value.
	 * 
	 * @param cfName
	 * @param key
	 * @param columnName
	 * @param value
	 * @param m
	 */
	public void generateNonCounter(String cfName, String key, String columnName, long value, MutationBatch m) {
		m.withRow(this.accessColumnFamily(cfName), key).putColumnIfNotNull(columnName, value);
	}

	/**
	 * Generate Mutation batch with Integer value.
	 * 
	 * @param cfName
	 * @param key
	 * @param columnName
	 * @param value
	 * @param m
	 */
	public void generateNonCounter(String cfName, String key, String columnName, Integer value, MutationBatch m) {
		m.withRow(this.accessColumnFamily(cfName), key).putColumnIfNotNull(columnName, value);
	}

	/**
	 * Generate Mutation batch with Boolean value.
	 * 
	 * @param cfName
	 * @param key
	 * @param columnName
	 * @param value
	 * @param m
	 */
	public void generateNonCounter(String cfName, String key, String columnName, Boolean value, MutationBatch m) {
		m.withRow(this.accessColumnFamily(cfName), key).putColumnIfNotNull(columnName, value);
	}

	/**
	 * Generate Mutation batch with Long value with expire time.
	 * 
	 * @param cfName
	 * @param key
	 * @param columnName
	 * @param value
	 * @param expireTime
	 * @param m
	 */
	public void generateTTLColumns(String cfName, String key, String columnName, long value, int expireTime, MutationBatch m) {
		m.withRow(this.accessColumnFamily(cfName), key).putColumnIfNotNull(columnName, value, expireTime);
	}

	/**
	 * Generate Mutation batch with String value with expire time.
	 * 
	 * @param cfName
	 * @param key
	 * @param columnName
	 * @param value
	 * @param expireTime
	 * @param m
	 */
	public void generateTTLColumns(String cfName, String key, String columnName, String value, int expireTime, MutationBatch m) {
		m.withRow(this.accessColumnFamily(cfName), key).putColumn(columnName, value, expireTime);
	}

	/**
	 * Delete all the data from ColumnFamily
	 * 
	 * @param cfName
	 */
	public void deleteAll(String cfName) {
		try {
			getKeyspace().truncateColumnFamily(this.accessColumnFamily(cfName));
		} catch (Exception e) {
			logger.error("Exception:",e);
		}
	}

	/**
	 * Delete single row key
	 * 
	 * @param cfName
	 * @param key
	 */
	public void deleteRowKey(String cfName, String key) {
		MutationBatch m = getKeyspace().prepareMutationBatch();
		try {
			m.withRow(this.accessColumnFamily(cfName), key).delete();
			m.execute();
			logger.info("Deleted row key - {}",key);
		} catch (Exception e) {
			logger.info("Failed to delete row key : {}",key);
			logger.error("Exception:",e);
		}
	}

	/**
	 * Delete a column
	 * 
	 * @param cfName
	 * @param key
	 * @param columnName
	 */
	public void deleteColumn(String cfName, String key, String columnName) {
		MutationBatch m = getKeyspace().prepareMutationBatch();
		try {
			m.withRow(this.accessColumnFamily(cfName), key).deleteColumn(columnName);
			m.execute();
			logger.info("Deleted row key : {} and column : {}",key,columnName);
		} catch (Exception e) {
			logger.info("Failed to delete row key : {} and column : {}",key,columnName);
			logger.error("Exception:",e);
		}
	}

	/**
	 * Get ColumnFamily class
	 * 
	 * @param columnFamilyName
	 * @return
	 */
	public ColumnFamily<String, String> accessColumnFamily(String columnFamilyName) {

		ColumnFamily<String, String> aggregateColumnFamily;

		aggregateColumnFamily = new ColumnFamily<String, String>(columnFamilyName, StringSerializer.get(), StringSerializer.get());

		return aggregateColumnFamily;
	}


	/**
	 * Saving data into event_timeline columnfamily
	 * 
	 * @param cfName
	 * @param rowKey
	 * @param CoulmnValue
	 * @param event
	 */
	public void updateTimelineObject(String cfName, String rowKey, String CoulmnValue, Event event) {

		// UUID eventColumnTimeUUID = TimeUUIDUtils.getUniqueTimeUUIDinMillis();

		MutationBatch eventTimeline = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5));

		eventTimeline.withRow(this.accessColumnFamily(cfName), rowKey).putColumn(CoulmnValue, CoulmnValue, null);

		eventTimeline.withRow(this.accessColumnFamily(cfName), (rowKey + SEPERATOR + event.getEventName())).putColumn(CoulmnValue, CoulmnValue, null);

		eventTimeline.withRow(this.accessColumnFamily(cfName), (rowKey.substring(0, 8) + SEPERATOR + event.getEventName())).putColumn(CoulmnValue, CoulmnValue, null);

		try {
			eventTimeline.execute();
		} catch (Exception e) {
			logger.error("Exception:",e);
		}
	}
	/**
	 * 
	 * @param is
	 * @return
	 */
	protected static String convertStreamToString(InputStream is) {
		BufferedReader reader = new BufferedReader(new InputStreamReader(is));
		StringBuilder sb = new StringBuilder();

		String line = null;
		try {
			while ((line = reader.readLine()) != null) {
				sb.append(line + "\n");
			}
		} catch (IOException e) {
			logger.error("Exception:",e);
		} finally {
			try {
				is.close();
			} catch (Exception e) {
				logger.error("Exception:",e);
			}
		}
		return sb.toString();
	}

}
