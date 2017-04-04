package com.datastax.global.test;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.demo.service.GlobalStoreService;
import com.datastax.demo.utils.PropertyHelper;
import com.datastax.demo.utils.Timer;
import com.datastax.timeseries.model.ObjectData;

public class GlobalStoreServiceLoadTest {

	private Logger logger = LoggerFactory.getLogger(GlobalStoreServiceLoadTest.class);
	public static String KEY = "somebigkeywithlotsofcharacters";
	private static int NO_OF_THREADS = 10;
	private AtomicLong emptys = new AtomicLong(0);
	private GlobalStoreService service;
	private BlockingQueue<ObjectData> queue = new ArrayBlockingQueue<ObjectData>(1000);
	private BlockingQueue<String> queueKey = new ArrayBlockingQueue<String>(1000);
	private ExecutorService pool;

	public GlobalStoreServiceLoadTest(String type) {
		if (type.equalsIgnoreCase("load")) {
			load();
		} else {
			runRandom();
		}
	}

	public void load() {
		service = new GlobalStoreService();

		int noOfKeys = Integer.parseInt(PropertyHelper.getProperty("noOfKeys", "1000000000"));

		pool = Executors.newFixedThreadPool(NO_OF_THREADS);

		for (int i = 0; i < NO_OF_THREADS; i++) {
			pool.submit(new Runnable() { 
				@Override
				public void run() {
					// TODO Auto-generated method stub
					while (true) {
						ObjectData object;
						try {
							object = queue.take();
							if (object != null) {
								service.putObjectData(object);
							}
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}

					}
				}
			});
		}

		for (int i = 812500000; i < noOfKeys; i++) {
			if (i % 100000 == 0) {
				logger.info("Inserted : " + (i) + " keys");
			}

			try {
				String key =KEY + "-" + (i + 1);
				queue.put(new ObjectData("" + key.hashCode() % 10000000, key));
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public void runRandom() {
		service = new GlobalStoreService();
		
		int noOfKeys = Integer.parseInt(PropertyHelper.getProperty("noOfKeys", "1000000000"));
		int noOfGets = Integer.parseInt(PropertyHelper.getProperty("noOfGets", "1000000"));

		pool = Executors.newFixedThreadPool(NO_OF_THREADS);

		for (int i = 0; i < NO_OF_THREADS; i++) {
			pool.submit(new Runnable() {
				@Override
				public void run() {
					// TODO Auto-generated method stub
					while (true) {
						String key;
						try {
							key = queueKey.take();
							if (key!= null) {
								if (service.getObjectData(key).isEmpty()){
									emptys.incrementAndGet();
								}
							}
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
				}
			});
		} 
		
		Timer timer = new Timer();
		for (int i = 0; i < noOfGets; i++) {

			String randomKey = KEY + "-" + new Double(Math.random() * noOfKeys + 1).intValue();
			try {
				queueKey.put(randomKey);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		timer.end();
		logger.info(String.format("%d took %d seconds (%d a sec). No of emptys %d", noOfGets, timer.getTimeTakenSeconds(), noOfGets/timer.getTimeTakenSeconds(), emptys.get()));
	}

	public static void main(String args[]) {
		new GlobalStoreServiceLoadTest(PropertyHelper.getProperty("type", "load1"));
	}
}
