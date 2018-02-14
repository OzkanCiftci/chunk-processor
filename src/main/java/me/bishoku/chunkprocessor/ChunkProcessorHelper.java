package me.bishoku.chunkprocessor;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ChunkProcessorHelper<T> {

	private final ExecutorService executor;
	private List<Future<Long>> taskList;

	private final int workerCount;
	private final int numElements;
	private final long timeout;
	private final TimeUnit unit;
	private RecordProcessor recordProcessor;
	private ChunkProcessor chunkProcessor;
	private final AtomicBoolean stopRequested;

	public ChunkProcessorHelper(int workerCount, int numElements, long timeout, TimeUnit unit) {
		this.workerCount = workerCount;
		this.numElements = numElements;
		this.timeout = timeout;
		this.unit = unit;
		this.executor = Executors.newFixedThreadPool(workerCount);
		this.stopRequested = new AtomicBoolean(false);
	}

	public ChunkProcessorHelper(int workerCount, int numElements, long timeout, TimeUnit unit,
			RecordProcessor recordProcessor) {
		this(workerCount, numElements, timeout, unit);
		this.recordProcessor = recordProcessor;

	}

	public ChunkProcessorHelper(int workerCount, int numElements, long timeout, TimeUnit unit,
			ChunkProcessor chunkProcessor) {
		this(workerCount, numElements, timeout, unit);
		this.chunkProcessor = chunkProcessor;
	}

	public static BlockingQueue source = new LinkedBlockingQueue<>();

	public void start() {
		taskList = IntStream.range(0, workerCount).mapToObj(i -> {
			if (recordProcessor != null)
				return new ChunkWorker(numElements, timeout, unit, recordProcessor, stopRequested);
			if (chunkProcessor != null)
				return new ChunkWorker(numElements, timeout, unit, chunkProcessor, stopRequested);
			throw new IllegalArgumentException("recordProcessor and chunkProcessor are both null");
		}).map(executor::submit).collect(Collectors.toList());
	}

	public void add(T t) throws InterruptedException {
		source.put(t);
	}

	public long stop() {
		stopRequested.set(true);
		long totalProcessed = taskList.stream().map(f -> {
			try {
				return f.get();
			} catch (InterruptedException | ExecutionException e) {
				return 0;
			}
		}).mapToLong(l -> l.longValue()).sum();
		executor.shutdown();
		return totalProcessed;
	}

	@FunctionalInterface
	public interface RecordProcessor<T> {
		public void process(T t);
	}

	@FunctionalInterface
	public interface ChunkProcessor<T> {
		public void process(List<T> t);
	}

}
