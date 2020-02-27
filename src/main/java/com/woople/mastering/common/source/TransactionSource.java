package com.woople.mastering.common.source;

import com.woople.mastering.common.entity.Transaction;
import org.apache.flink.annotation.Public;
import org.apache.flink.streaming.api.functions.source.FromIteratorFunction;

import java.io.Serializable;
import java.util.Iterator;

/**
 * A stream of transactions.
 */
@Public
public class TransactionSource extends FromIteratorFunction<Transaction> {

	private static final long serialVersionUID = 1L;

	public TransactionSource() {
		super(new RateLimitedIterator<>(TransactionIterator.unbounded()));
	}

	private static class RateLimitedIterator<T> implements Iterator<T>, Serializable {

		private static final long serialVersionUID = 1L;

		private final Iterator<T> inner;

		private RateLimitedIterator(Iterator<T> inner) {
			this.inner = inner;
		}

		@Override
		public boolean hasNext() {
			return inner.hasNext();
		}

		@Override
		public T next() {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
			return inner.next();
		}
	}
}
