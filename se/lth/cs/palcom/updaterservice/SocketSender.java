package se.lth.cs.palcom.updaterservice;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.ConnectException;
import java.net.Socket;
import java.net.UnknownHostException;

import se.lth.cs.palcom.logging.Logger;

public class SocketSender {
	public static final int TRY_FOREVER = -1;
	private int sendPort;
	private Socket s;
	private UpdaterService us;
	private long defaultRetryDelayMillis = 100;
	public SocketSender(UpdaterService us, int sendPort) {
		this.sendPort = sendPort;
		this.us = us;
	}
	/**
	 * 
	 * @param msg
	 * @param waitInSeconds SocketSender.TRY_FOREVER (-1) to try forever
	 */
	public boolean sendMsg(String msg, int waitInSeconds) {
		long stopTimeMillis = System.currentTimeMillis() + waitInSeconds*1000;
		boolean connected = false;
		while (!connected) {
			try {
				connected = true;
				s = new Socket("localhost", sendPort);
			} catch (ConnectException e) {
				connected = false;
				long timeLeftToStopMillis = stopTimeMillis - System.currentTimeMillis();
				if (waitInSeconds != -1 && timeLeftToStopMillis < 0) {
					us.log("SocketSender could not connect to localhost port " + sendPort, Logger.CMP_SERVICE, Logger.LEVEL_WARNING);
					return false;
				}
				long msToWait = defaultRetryDelayMillis < timeLeftToStopMillis ? defaultRetryDelayMillis : timeLeftToStopMillis;
				us.log("SocketSender could not connect to localhost port " + sendPort + ". Trying again in " + msToWait + "ms.", Logger.CMP_SERVICE, Logger.LEVEL_DEBUG);
				try {
					Thread.sleep(msToWait);
				} catch (InterruptedException e1) {/* do nothing */}
			} catch (UnknownHostException e) {
				us.log("SocketSender: Unknown host.", Logger.CMP_SERVICE, Logger.LEVEL_WARNING);
				return false;
			} catch (IOException e) {
				us.log("SocketSender: IOException: " + e.getMessage(), Logger.CMP_SERVICE, Logger.LEVEL_WARNING);
				return false;
			}			
		}
		
		BufferedWriter out;
		try {
			out = new BufferedWriter(new OutputStreamWriter(s.getOutputStream()));
			out.write(msg);
			us.log("Socket message sent: \"" + msg + "\"", Logger.CMP_SERVICE, Logger.LEVEL_DEBUG);
			out.newLine();
			out.flush();
			out.close();
			s.close();
		} catch (IOException e) {
			us.log("SocketSender could not send msg: " + msg  + ". IOException: " + e.getMessage(), Logger.CMP_SERVICE, Logger.LEVEL_WARNING);
			return false;
		}
		return true;
	}
}
