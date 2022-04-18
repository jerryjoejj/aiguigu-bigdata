package lma.lock1;

import org.apache.zookeeper.KeeperException;

import java.io.IOException;

public class DistributedLockTest {

	public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
		DistributedLock lock1 = new DistributedLock();
		DistributedLock lock2 = new DistributedLock();

		new Thread(() -> {
			try {
				lock1.zkLock();
				System.out.println("Thread 1 get the lock...");
				Thread.sleep(5 * 1000);
				lock1.zkUnLock();
				System.out.println("Thread 1 realse the lock...");
			} catch (InterruptedException | KeeperException e) {
				e.printStackTrace();
			}
		}).start();

		new Thread(() -> {
			try {
				lock2.zkLock();
				System.out.println("Thread 2 get the lock...");
				Thread.sleep(5 * 1000);
				lock2.zkUnLock();
				System.out.println("Thread 2 realse the lock...");
			} catch (InterruptedException | KeeperException e) {
				e.printStackTrace();
			}
		}).start();
	}

}
