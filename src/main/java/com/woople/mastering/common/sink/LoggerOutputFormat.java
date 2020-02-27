package com.woople.mastering.common.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A basic output format that logs all records at level <b>INFO</b>.
 */
@Internal
public class LoggerOutputFormat implements OutputFormat<String> {

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(LoggerOutputFormat.class);

	@Override
	public void configure(Configuration parameters) { }

	@Override
	public void open(int taskNumber, int numTasks) { }

	@Override
	public void writeRecord(String record) {
		LOG.info(record);
	}

	@Override
	public void close() { }
}
