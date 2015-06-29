package com.dontocsata.comskip.queue;

import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProcessWatchDog implements Runnable {

	private static final Logger log = LoggerFactory.getLogger(ProcessWatchDog.class);

	public static final int PROCESS_KILLED = -100;

	private ProgressReporter progress;
	private Process process;
	private Date start;
	private Date end;

	private Integer exitValue;

	private volatile Thread thread;
	private volatile boolean running = true;

	public ProcessWatchDog(Process process, ProgressReporter progress, long time, TimeUnit unit) {
		this.process = process;
		this.start = new Date();
		this.end = new Date(start.getTime() + unit.toMillis(time));
		this.progress = progress;
	}

	public void shutdown() {
		running = false;
		if (thread != null) {
			thread.interrupt();
		}
	}

	public Integer exitValue() {
		return exitValue;
	}

	@Override
	public void run() {
		thread = Thread.currentThread();
		while (running && !Thread.interrupted()) {
			if (new Date().after(end)) {
				process.destroy();
				exitValue = PROCESS_KILLED;
				break;
			} else {
				try {
					exitValue = process.exitValue();
					break;
				} catch (IllegalThreadStateException ex) {
					// no-op
				}
			}
			try {
				Thread.sleep(TimeUnit.SECONDS.toMillis(30));
			} catch (InterruptedException e) {

			}
			// log.debug("Progress: {}", progress.getProgress());
		}

		if (process != null) {
			process.destroy();
		}
	}
}
