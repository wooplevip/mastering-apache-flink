package com.woople.mastering.common.table;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * A user defined function for rounding timestamps down to
 * the nearest hour.
 */
@PublicEvolving
@SuppressWarnings("unused")
public class TruncateDateToHour extends ScalarFunction {

	private static final long serialVersionUID = 1L;

	private static final long ONE_HOUR = 60 * 60 * 1000;

	public long eval(long timestamp) {
		return timestamp - (timestamp % ONE_HOUR);
	}

	@Override
	public TypeInformation<?> getResultType(Class<?>[] signature) {
		return Types.SQL_TIMESTAMP;
	}
}
