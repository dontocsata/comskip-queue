package com.dontocsata.comskip.queue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Queueing extends TimerTask {

	public static final String SKIP_CONVERSION = "skip.conversion";
	public static final String RECORDED_DIR = "recorded.directory";
	public static final String TEMP_DIR = "temp.directory";
	public static final String COMMERCIAL_DIR = "commercial.directory";
	public static final String COMSKIP_EXE = "comskip.executable";
	public static final String COMSKIP_OPTS = "comskip.options";
	public static final String WTVCONVERT_EXE = "wtvconvert.executable";
	public static final String PROCESSED_FILE = "processed.file";
	public static final String WORKER_THREADS = "worker.threads";

	private static final Logger PROCESSED_LOGGER = LoggerFactory.getLogger("processed");
	private static final Logger log = LoggerFactory.getLogger(Queueing.class);

	public static void main(String[] args) throws Exception {
		Properties props = new Properties();
		String propFile = "config.properties";
		if (args.length > 1) {
			System.err.println("Incorrect args.");
			System.exit(1);
		} else if (args.length == 1) {
			propFile = args[0];
		}
		try (InputStream in = new FileInputStream(propFile)) {
			props.load(in);
		} catch (IOException ex) {
			ex.printStackTrace();
			System.exit(2);
		}
		final Queueing queueing = new Queueing(props);
		final Timer timer = new Timer();
		timer.scheduleAtFixedRate(queueing, 0, TimeUnit.MINUTES.toMillis(5));
		Runtime.getRuntime().addShutdownHook(new Thread() {

			@Override
			public void run() {
				timer.cancel();
				queueing.shutdown();
			}
		});
	}

	private Properties props;
	private LinkedBlockingDeque<File> workQueue = new LinkedBlockingDeque<>();

	private Set<String> processed = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
	private Set<String> processing = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

	private Set<String> toBeDeleted = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

	private ConcurrentLinkedQueue<Worker> workers = new ConcurrentLinkedQueue<>();
	private ConcurrentLinkedQueue<ProcessWatchDog> watchDogs = new ConcurrentLinkedQueue<>();

	public Queueing(Properties props) throws IOException {
		this.props = props;
		if (new File(props.getProperty(PROCESSED_FILE)).exists()) {
			try (BufferedReader in = new BufferedReader(new FileReader(props.getProperty(PROCESSED_FILE)))) {
				String line;
				while ((line = in.readLine()) != null) {
					processed.add(line);
				}
			}
		}
		int threads = Integer.parseInt(props.getProperty(WORKER_THREADS, "1"));

		for (int i = 0; i < threads; i++) {
			Worker w = new Worker();
			workers.add(w);
			new Thread(w, "Worker-" + (i + 1)).start();
		}
	}

	public void shutdown() {
		for (Worker worker : workers) {
			worker.running = false;
		}
		for (ProcessWatchDog pwd : watchDogs) {
			pwd.shutdown();
		}
	}

	@Override
	public void run() {
		File temp = new File(props.getProperty(TEMP_DIR));
		if (!temp.exists()) {
			temp.mkdirs();
		}
		File recordedDir = new File(props.getProperty(RECORDED_DIR));
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.MINUTE, -10);
		final Date date = cal.getTime();
		File[] files = recordedDir.listFiles(new FileFilter() {

			@Override
			public boolean accept(File pathname) {
				return new Date(pathname.lastModified()).before(date) && pathname.getName().endsWith(".wtv")
						&& !processed.contains(pathname.getAbsolutePath())
						&& !processing.contains(pathname.getAbsolutePath());
			}

		});
		for (File file : files) {
			processing.add(file.toString());
			workQueue.add(file);
		}
		log.info("Work Queue size={}", workQueue.size());
		for (Iterator<String> it = toBeDeleted.iterator(); it.hasNext();) {
			File f = new File(it.next());
			if (f.delete()) {
				it.remove();
			} else {
				log.warn("Could not delete {}", f);
			}
		}
	}

	private class Worker implements Runnable {

		private volatile boolean running = true;

		@Override
		public void run() {
			while (running) {
				File file = null;
				try {
					file = workQueue.take();
					File commercialFile = new File(
							props.getProperty(COMMERCIAL_DIR) + "/" + getFilenameWithoutExtension(file) + ".xml");
					if (commercialFile.exists()) {
						processed.add(file.getAbsolutePath());
						continue;
					}

					log.debug("Starting on: " + file);
					long start = System.currentTimeMillis();
					String name = getFilenameWithoutExtension(file);
					File toComskipFile = file;
					Integer toRet = 0;
					boolean skipConvert = Boolean.parseBoolean(props.getProperty(SKIP_CONVERSION, "false"));
					if (!skipConvert) {
						// run wtvconverter
						String dvrmsFile = props.getProperty(TEMP_DIR) + "/" + name + ".dvr-ms";
						File dvrms = new File(dvrmsFile);
						if (!dvrms.exists()) {
							log.debug("Converting to DVR-MS: {}", file);
							String[] cmd = new String[] { props.getProperty(WTVCONVERT_EXE), file.getAbsolutePath(),
									dvrmsFile };
							Process process = Runtime.getRuntime().exec(cmd);
							toRet = process.waitFor();
						}
						if (toRet != 0) {
							log.error("Non-zero termination during conversion {} for {}", toRet, file);
							processing.remove(file.getAbsolutePath());
						} else if (dvrms.exists()) {
							toComskipFile = dvrms;
						}
					}
					if (toComskipFile.exists()) {
						log.debug("Running compskip on {}", toComskipFile);

						List<String> cmds = new ArrayList<>();
						cmds.add(props.getProperty(COMSKIP_EXE));
						for (String s : props.getProperty(COMSKIP_OPTS).split(" ")) {
							if (!s.isEmpty()) {
								cmds.add(s);
							}
						}
						cmds.add(toComskipFile.getAbsolutePath());
						cmds.add(props.getProperty(COMMERCIAL_DIR));

						String[] cmd = cmds.toArray(new String[cmds.size()]);

						String parentName = Thread.currentThread().getName();
						final Process proc = Runtime.getRuntime().exec(cmd);
						ComskipProgressReporter progressReporter = new ComskipProgressReporter(proc.getErrorStream());

						Thread errorStreamThread = new Thread(progressReporter, parentName + "-ErrorStream");
						errorStreamThread.start();
						Thread inputStreamThread = new Thread(new InputStreamRunnable(proc.getInputStream()),
								parentName + "-InputStream");
						inputStreamThread.start();
						// size is gigabytes
						double size = toComskipFile.length() / 1073741824.0;
						// 12 minutes + 10 minutes per gigabyte
						long time = 12 + 10 * (long) (size - 1);

						ProcessWatchDog pwd = new ProcessWatchDog(proc, progressReporter, time, TimeUnit.MINUTES);
						watchDogs.add(pwd);
						Thread watchdog = new Thread(pwd, parentName + "-WatchDog");
						watchdog.start();
						watchdog.join();
						watchDogs.remove(watchdog);
						toRet = pwd.exitValue();
						Thread.sleep(3000);
						proc.waitFor();
						if (toRet == 0 || toRet == 1 || toRet == ProcessWatchDog.PROCESS_KILLED) {
							PROCESSED_LOGGER.info(file.getAbsolutePath());
							processed.add(file.getAbsolutePath());
							processing.remove(file.getAbsolutePath());
							if (!skipConvert) {
								for (int i = 0; i < 5; i++) {
									if (toComskipFile.delete()) {
										break;
									}
								}
								if (toComskipFile.exists()) {
									toBeDeleted.add(toComskipFile.getAbsolutePath());
								}
							}
						} else if (commercialFile.exists()) {
							commercialFile.delete();
						}
						log.info("Completed comskip (exit code: {}) on {}", toRet, file);
						double howLong = (System.currentTimeMillis() - start) / (1000.0 * 60.0);
						log.info("File {} with size: {} took {} min", file, size, howLong);
					}
				} catch (Exception ex) {
					log.error("Exception on " + file.getAbsolutePath(), ex);
					if (file != null) {
						processing.remove(file.getAbsolutePath());
					}
				}
			}
		}
	}

	private static String getFilenameWithoutExtension(File f) {
		return f.getName().substring(0, f.getName().lastIndexOf('.'));
	}
}
