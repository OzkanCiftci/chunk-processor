package me.bishoku.chunkprocessor;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.TimeUnit;

import org.junit.Test;

import me.bishoku.chunkprocessor.ChunkProcessorHelper.ChunkProcessor;
import me.bishoku.chunkprocessor.ChunkProcessorHelper.RecordProcessor;

public class ChunkProcessorHelperTest {

	@Test
	public void basicStartProcessRecordByRecordStopTest() throws InterruptedException {
		RecordProcessor<String> recordProcessor = record -> {
			System.out.println(String.format("Thread %s, Record: %s", Thread.currentThread().getName(), record));
		};
		ChunkProcessorHelper<String> chunkProcessor = new ChunkProcessorHelper<>(2, 3, 1, TimeUnit.SECONDS,
				recordProcessor);

		chunkProcessor.start();
		for (int i = 0; i < 100; i++) {
			chunkProcessor.add(String.format("Record %03d", i));
		}
		long totalProcessed = chunkProcessor.stop();
		assertEquals(100, totalProcessed);
	}

	@Test
	public void basicStartProcessChunkByChunkStopTest() throws InterruptedException {
		ChunkProcessor<String> chunkProcessor = record -> {
			System.out.println(String.format("%s Processing chunk----,", Thread.currentThread().getName()));
			record.stream().forEach(System.out::println);
		};
		ChunkProcessorHelper<String> chunkProcessorHelper = new ChunkProcessorHelper<>(2, 3, 1, TimeUnit.SECONDS,
				chunkProcessor);

		chunkProcessorHelper.start();
		for (int i = 0; i < 100; i++) {
			chunkProcessorHelper.add(String.format("Record %03d", i));
		}
		long totalProcessed = chunkProcessorHelper.stop();
		assertEquals(100, totalProcessed);
	}

	@Test
	public void processHeavyRecordsWithMoreThreadTest() throws InterruptedException {
		RecordProcessor<String> recordProcessor = record -> {
			System.out.println(String.format("Thread %s, Record: %s", Thread.currentThread().getName(), record));
			try {
				TimeUnit.SECONDS.sleep(1);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		};
		ChunkProcessorHelper<String> chunkProcessor = new ChunkProcessorHelper<>(20, 100, 200, TimeUnit.MILLISECONDS,
				recordProcessor);

		chunkProcessor.start();
		for (int i = 0; i < 1000; i++) {
			chunkProcessor.add(String.format("Record %03d", i));
		}
		long totalProcessed = chunkProcessor.stop();
		assertEquals(1000, totalProcessed);
	}

}
