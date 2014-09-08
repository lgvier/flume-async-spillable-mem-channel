package org.apache.flume.channel;

import org.apache.flume.Context;
import org.apache.flume.channel.AsyncSpillableMemoryChannel;
import org.apache.flume.event.SimpleEvent;

/**
 * Just a simple sandbox class to evaluate the performance of the {@link AsyncSpillableMemoryChannel}. <br/>
 * 
 * @author lvier
 */
public class AsyncSpillableMemoryChannelPerformanceTest {

	public static void main(String[] args) throws Exception {
		
		final AsyncSpillableMemoryChannel channel = new AsyncSpillableMemoryChannel();
		channel.setName("pftest1");
		Context ctx = new Context();
		ctx.put("capacity", "20000000");
		ctx.put("transactionCapacity", "10000");
		ctx.put("keep-alive","0");
		
		ctx.put("useDualCheckpoints", "true");
		ctx.put("backupCheckpointDir", "/Users/lvier/.flume/file-channel/checkpoint-bkp");
		
		channel.configure(ctx);
		channel.start();
		
		// Consumer (starts slower than the producer but eventually catches up)
		new Thread() {
			public void run() {
				
				long time = 200;
				while (true) {
					channel.getTransaction().begin();
					for (int i = 0; i < 1000; i++) {
						channel.take();
					}
					channel.getTransaction().commit();
					channel.getTransaction().close();
					try {
						if (time > 0) {
							Thread.sleep(time--);
						}
					} catch (InterruptedException e) { }
				}
				
				
			}
		}.start();
		
		StringBuilder msg = new StringBuilder();
		for (int i = 0; i < 50; i++) {
			msg.append("tst").append(i);
		}
		
		long dataCount = 0;
		long start = System.currentTimeMillis();
		int lastLogIndex = 0;
		long lastLogTime = 0;
		
		boolean transactionIsOpen = false;
		
		// Producer
		
		for (int i = 0; i < 1000000000; i++) {
			byte[] data = (i + msg.toString()).getBytes();
			// queueFile.add(data);
			//os.write(data);
			if (!transactionIsOpen) {
				channel.getTransaction().begin();
				transactionIsOpen = true;
			}
			SimpleEvent event = new SimpleEvent();
			event.setBody(data);
			channel.put(event);
			
			if (i % 1000 == 0) {
				channel.getTransaction().commit();
				channel.getTransaction().close();
				transactionIsOpen = false;
				Thread.sleep(20);
			}
			
			dataCount += data.length;
			
			
			if (i - lastLogIndex > 1000) {
				lastLogIndex = i;
				
				long now = System.currentTimeMillis();
				if (now - lastLogTime > 10000) {
					double seconds = (now - start) / 1000.d;
					double mb = dataCount / 1024.d / 1024.d;
					System.out.println(mb / seconds + " MB per second");
					lastLogTime = now;
				}
			}
			
		}
		
		channel.stop();
	}
	
}
