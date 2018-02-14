package me.bishoku.chunkprocessor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import me.bishoku.chunkprocessor.ChunkProcessorHelper.ChunkProcessor;
import me.bishoku.chunkprocessor.ChunkProcessorHelper.RecordProcessor;
import me.bishoku.chunkprocessor.util.QueueUtil;

public class ChunkWorker implements Callable<Long> {

	private final int numElements;
	private final long timeout;
	private final TimeUnit unit;
	private RecordProcessor recordProcessor;
	private ChunkProcessor chunkProcessor;
	private final AtomicBoolean stopRequested;

	public ChunkWorker(int numElements, long timeout, TimeUnit unit, RecordProcessor recordProcessor,
			AtomicBoolean stopRequested) {
		super();
		this.numElements = numElements;
		this.timeout = timeout;
		this.unit = unit;
		this.recordProcessor = recordProcessor;
		this.stopRequested = stopRequested;
	}

	public ChunkWorker(int numElements, long timeout, TimeUnit unit, ChunkProcessor chunkProcessor,
			AtomicBoolean stopRequested) {
		super();
		this.numElements = numElements;
		this.timeout = timeout;
		this.unit = unit;
		this.chunkProcessor = chunkProcessor;
		this.stopRequested = stopRequested;
	}

	@Override
	public Long call() throws Exception {
		long processed = 0;
		boolean recordExist = true;
		while (!Thread.currentThread().isInterrupted() && recordExist) {
			try {
				List chunk = new ArrayList<>();
				int drained = QueueUtil.drain(ChunkProcessorHelper.source, chunk, numElements, timeout, unit);
				if (drained > 0) {
					if (recordProcessor != null) {
						chunk.stream().forEach(recordProcessor::process);
					} else if (chunkProcessor != null) {
						chunkProcessor.process(chunk);
					}
					processed += drained;
				} else if (stopRequested.get()) {
					recordExist = false;
				}
			} catch (InterruptedException ie) {
				Thread.currentThread().interrupted();
			}
		}
		return processed;
	}

}
