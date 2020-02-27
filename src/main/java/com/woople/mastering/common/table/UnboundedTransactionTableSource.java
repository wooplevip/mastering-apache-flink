package com.woople.mastering.common.table;

import com.woople.mastering.common.entity.Transaction;
import com.woople.mastering.common.source.TransactionSource;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.DefinedRowtimeAttributes;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.tsextractors.ExistingField;
import org.apache.flink.table.sources.wmstrategies.BoundedOutOfOrderTimestamps;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import java.sql.Timestamp;
import java.util.Collections;
import java.util.List;

/**
 * A table source for reading an unbounded set of transactions.
 *
 * <p>This table could be backed by a message queue or other streaming data source.
 */
@PublicEvolving
@SuppressWarnings({"deprecation", "unused"})
public class UnboundedTransactionTableSource
		implements StreamTableSource<Row>,
        DefinedRowtimeAttributes {

	@Override
	public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
		return execEnv
			.addSource(new TransactionSource())
			.map(transactionRowMapFunction())
			.returns(getTableSchema().toRowType());
	}

	private MapFunction<Transaction, Row> transactionRowMapFunction() {
		return transaction -> Row.of(
			transaction.getAccountId(),
			new Timestamp(transaction.getTimestamp()),
			transaction.getAmount());
	}

	@Override
	public DataType getProducedDataType() {
		return getTableSchema().toRowDataType();
	}

	@Override
	public TableSchema getTableSchema() {
		return TableSchema.builder()
			.field("accountId", Types.LONG)
			.field("timestamp", Types.SQL_TIMESTAMP)
			.field("amount", Types.DOUBLE)
			.build();
	}

	@Override
	public List<RowtimeAttributeDescriptor> getRowtimeAttributeDescriptors() {
		return Collections.singletonList(
			new RowtimeAttributeDescriptor(
				"timestamp",
				new ExistingField("timestamp"),
				new BoundedOutOfOrderTimestamps(100)));
	}
}
