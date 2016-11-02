package com.ajk.dw.scheduler.worker;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import com.ajk.dw.scheduler.log.SchedulerLogger;


class StreamLogger extends Thread {
    final InputStream is;
    final SchedulerLogger droneLogger;
    final boolean isStdout;

    public StreamLogger(String name, InputStream inputStream,
            SchedulerLogger logger, boolean isStdout) {
        this.setName(name);
        this.is = inputStream;
        this.droneLogger = logger;
        this.isStdout = isStdout;
    }

    @Override
	public void run() {
        BufferedReader br = null;
        try {
            br = new BufferedReader(new InputStreamReader(is));
            String line = null;

            while ((line = br.readLine()) != null) {
                if (isStdout) {
                    droneLogger.log(line);
                } else {
                    droneLogger.err(line);
                }
            }
        } catch (IOException ioe) {
            droneLogger.err("Error consuming stream of spawned process.", ioe);
        } finally {
        	try {
				br.close();
			} catch (IOException e) {
				droneLogger.err("Error close bufferadReader.", e);
			}
        }
    }
}
