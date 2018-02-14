package me.bishoku.chunkprocessor.util;

import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class QueueUtil {
	
	public static <E> int drain(BlockingQueue<E> q, Collection<? super E> buffer, int numElements, long timeout,
			TimeUnit unit) throws InterruptedException {

		/*
		 * This code performs one System.nanoTime() more than necessary, and in return,
		 * the time to execute Queue#drainTo is not added *on top* of waiting for the
		 * timeout (which could make the timeout arbitrarily inaccurate, given a queue
		 * that is slow to drain).
		 */
		long deadline = System.nanoTime() + unit.toNanos(timeout);
		int added = 0;
		while (added < numElements) {
			// we could rely solely on #poll, but #drainTo might be more efficient when
			// there are multiple
			// elements already available (e.g. LinkedBlockingQueue#drainTo locks only once)
			added += q.drainTo(buffer, numElements - added);
			if (added < numElements) { // not enough elements immediately available; will have to poll
				E e = q.poll(deadline - System.nanoTime(), TimeUnit.NANOSECONDS);
				if (e == null) {
					break; // we already waited enough, and there are no more elements in sight
				}
				buffer.add(e);
				added++;
			}
		}
		return added;
	}

	/**
	 * Drains the queue as
	 * {@linkplain #drain(BlockingQueue, Collection, int, long, TimeUnit)}, but with
	 * a different behavior in case it is interrupted while waiting. In that case,
	 * the operation will continue as usual, and in the end the thread's
	 * interruption status will be set (no {@code
	 * InterruptedException} is thrown).
	 *
	 * @param q
	 *            the blocking queue to be drained
	 * @param buffer
	 *            where to add the transferred elements
	 * @param numElements
	 *            the number of elements to be waited for
	 * @param timeout
	 *            how long to wait before giving up, in units of {@code unit}
	 * @param unit
	 *            a {@code TimeUnit} determining how to interpret the timeout
	 *            parameter
	 * @return the number of elements transferred
	 */

	public static <E> int drainUninterruptibly(BlockingQueue<E> q, Collection<? super E> buffer, int numElements,
			long timeout, TimeUnit unit) {

		long deadline = System.nanoTime() + unit.toNanos(timeout);
		int added = 0;
		boolean interrupted = false;
		try {
			while (added < numElements) {
				// we could rely solely on #poll, but #drainTo might be more efficient when
				// there are
				// multiple elements already available (e.g. LinkedBlockingQueue#drainTo locks
				// only once)
				added += q.drainTo(buffer, numElements - added);
				if (added < numElements) { // not enough elements immediately available; will have to poll
					E e; // written exactly once, by a successful (uninterrupted) invocation of #poll
					while (true) {
						try {
							e = q.poll(deadline - System.nanoTime(), TimeUnit.NANOSECONDS);
							break;
						} catch (InterruptedException ex) {
							interrupted = true; // note interruption and retry
						}
					}
					if (e == null) {
						break; // we already waited enough, and there are no more elements in sight
					}
					buffer.add(e);
					added++;
				}
			}
		} finally {
			if (interrupted) {
				Thread.currentThread().interrupt();
			}
		}
		return added;
	}
}
