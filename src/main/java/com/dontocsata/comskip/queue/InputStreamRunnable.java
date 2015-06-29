package com.dontocsata.comskip.queue;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InputStreamRunnable implements Runnable {

	private static final Logger log = LoggerFactory
			.getLogger(InputStreamRunnable.class);

	private InputStream in;
	private StringBuilder sb = new StringBuilder();

	public InputStreamRunnable(InputStream in) {
		this.in = in;
	}

	@Override
	public String toString() {
		return sb.toString();
	}

	@Override
	public void run() {
		try (BufferedInputStream bis = new BufferedInputStream(in)) {
			int c;
			while ((c = in.read()) > -1) {
				sb.append((char) c);
			}
		} catch (IOException ex) {
			log.warn("IOException while reading stream.", ex);
		}

	}

}
