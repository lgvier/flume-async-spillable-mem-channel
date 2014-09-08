package org.apache.flume.channel;

import java.lang.reflect.Field;
import java.util.Queue;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.channel.file.FileChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A channel that spills to disk asynchronously.<br/>
 * Not ready for production
 * 
 * @author lgvier
 */
public class AsyncSpillableMemoryChannel extends MemoryChannel {
	
	private static final Logger log = LoggerFactory.getLogger(AsyncSpillableMemoryChannel.class);
	
	protected int transactionCapacity;
	protected int defaultTransactionCapacity = 1000;
	// FIXME make configurable
	protected int timerInterval = 1000;
	/** When to start spilling to disk */
	protected int memoryChannelSoftCapacity = 1000;
	/** When to start discarding old events */
	protected int fileChannelSoftCapacity = 10000000;
	protected int fileChannelCapacity = 20000000;
	
	protected InternalFileChannel fileChannel;
	protected Timer timer; // TODO use wait/notify on puts an takes instead
	protected Queue<Event> memoryQueue;

	public AsyncSpillableMemoryChannel() {
		this.fileChannel = new InternalFileChannel();
	}

	@Override
	public synchronized void setName(String name) {
		super.setName(name);
		this.fileChannel.setName(name + "-overflow");
	}
	
	@Override
	public void configure(Context context) {
		
		try {
			Integer transCapacityProperty = context.getInteger("transactionCapacity");
			if (transCapacityProperty == null) {
				this.transactionCapacity = defaultTransactionCapacity;
				context.put("transactionCapacity", String.valueOf(transactionCapacity));
			} else {
				this.transactionCapacity = transCapacityProperty;
			}
		} catch(NumberFormatException e) {
			this.transactionCapacity = defaultTransactionCapacity;
			context.put("transactionCapacity", String.valueOf(transactionCapacity));
			log.warn("Invalid transation capacity specified, initializing channel"
					+ " to default capacity of {}", defaultTransactionCapacity);
		}
		
		super.configure(context);
		context.put("capacity", String.valueOf(fileChannelCapacity));
		context.put("keep-alive","0");
		fileChannel.configure(context);
	}
	
	@Override
	public synchronized void start() {
		super.start();
		this.obtainMemoryQueueReference();
		this.timer = new Timer(true);
		this.timer.schedule(new TimerTask() {
			@Override
			public void run() {
				startFileChannel();
			}
		}, 10);
	}
	
	protected void startFileChannel() {
		log.info("Starting the file channel...");
		this.fileChannel.start();
		this.timer.scheduleAtFixedRate(new TimerTask() {
			@Override
			public void run() {
				doBackgroundWork();
			}
		}, timerInterval, timerInterval);
		log.info("Started the file channel.");
	}
	
	@Override
	public synchronized void stop() {
		this.timer.cancel();
		this.fileChannel.stop();
		super.stop();
	}
	
	// TODO refactor into smaller methods
	protected void doBackgroundWork() {
		int memQueueSize;
		boolean spilledToDisk = false;
		while ((memQueueSize = memoryQueue.size()) > memoryChannelSoftCapacity) {
			// will spill to disk
			
			// TODO optional functionality - move to a Listener
			// Discard older events from the FileChannel if necessary
			int fileQueueSize;
			while ((fileQueueSize = fileChannel.getDepth()) > fileChannelSoftCapacity) {
				log.debug("Discarding older FileChannel events ({} > {})", fileQueueSize, fileChannelSoftCapacity);
				Transaction fileChannelTransaction = this.fileChannel.getTransaction();
				fileChannelTransaction.begin();
				try {
					for (int i = 0; i < this.transactionCapacity; i++) {
						Event event = this.fileChannel.take();
						if (event == null)
							break;
					}
					fileChannelTransaction.commit();
				} finally {
					fileChannelTransaction.close();
				}
			}
			
			// Move events from the MemoryChannel to the FileChannel
			
			log.info("Spilling to disk... Memory queue size: {}, File queue size: {}", memQueueSize, fileQueueSize);
			Transaction fileChannelTransaction = this.fileChannel.getTransaction();
			Transaction memoryChannelTransaction = this.getTransaction();
			fileChannelTransaction.begin();
			memoryChannelTransaction.begin();
			try {
				for (int i = 0; i < this.transactionCapacity; i++) {
					Event event = this.take();
					if (event == null)
						break;
					this.fileChannel.put(event);
				}
				fileChannelTransaction.commit();
				memoryChannelTransaction.commit();
			} finally {
				fileChannelTransaction.close();
				memoryChannelTransaction.close();
			}
			spilledToDisk = true;
		}
		
		if (!spilledToDisk) {
			int fileQueueSize;
			while ((fileQueueSize = fileChannel.getDepth()) > 0 && (memQueueSize = memoryQueue.size()) < (memoryChannelSoftCapacity / 2)) {
				// Move events back to the Memory channel
				
				log.info("Restoring from disk... File queue size: {}, Memory queue size: {}", fileQueueSize, memQueueSize);
				
				Transaction fileChannelTransaction = this.fileChannel.getTransaction();
				Transaction memoryChannelTransaction = this.getTransaction();
				fileChannelTransaction.begin();
				memoryChannelTransaction.begin();
				
				try {
					for (int i = 0; i < this.transactionCapacity; i++) {
						Event event = this.fileChannel.take();
						if (event == null)
							break;
						this.put(event);
					}
					fileChannelTransaction.commit();
					memoryChannelTransaction.commit();
				} finally {
					fileChannelTransaction.close();
					memoryChannelTransaction.close();
				}
			}
		}
		log.debug("doBackgroundWork() leaving...");
	}
	
	/**
	 * FIXME expose the queue's depth on the MemoryChannel
	 */
	@SuppressWarnings("unchecked")
	protected void obtainMemoryQueueReference() {
		try {
			Field queueField = MemoryChannel.class.getDeclaredField("queue");
			queueField.setAccessible(true);
			memoryQueue = (Queue<Event>) queueField.get(this);
		} catch (Exception e) {
			throw new IllegalStateException("Cannot obtain a reference to the Queue", e);
		}
	}
	
	/**
	 * Make the {@link #getDepth()} method public
	 */
	protected static class InternalFileChannel extends FileChannel {
		
		@Override
		public int getDepth() {
			return super.getDepth();
		}
		
	}
	
}
