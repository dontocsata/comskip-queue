package com.dontocsata.comskip.queue;

import java.io.InputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ComskipProgressReporter extends InputStreamRunnable implements
		ProgressReporter {

	private static final Logger log = LoggerFactory
			.getLogger(ComskipProgressReporter.class);

	public ComskipProgressReporter(InputStream in) {
		super(in);
	}

	@Override
	public Integer getProgress() {
		String s = super.toString();
		log.debug("Comskip Progress: {}", s);
		if (s != null) {
			int end = s.lastIndexOf("%");
			if (end == -1) {
				return null;
			}
			int start = end - 3;
			String percent = s.substring(start, end).trim();
			log.debug("Percent: " + percent);
			try {
				return Integer.parseInt(percent);
			} catch (NumberFormatException ex) {

			}
		}
		return null;
	}

}
