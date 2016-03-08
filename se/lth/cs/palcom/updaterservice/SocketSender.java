package se.lth.cs.palcom.updaterservice;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.ConnectException;
import java.net.Socket;
import java.net.UnknownHostException;

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
	 * @param waitInSeconds, TRY_FOREVER (-1) to try forever
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
				long timeLeftToStopMillis = stopTimeMillis - System.currentTimeMillis(); // get time left until we wont try anymore
				if (waitInSeconds != -1 && timeLeftToStopMillis < 0) {
					us.printderp("SocketSender could not connect to localhost port " + sendPort);
					return false;
				}
				long msToWait = defaultRetryDelayMillis < timeLeftToStopMillis ? defaultRetryDelayMillis : timeLeftToStopMillis;// wait the least amount of time
				us.printderp("SocketSender could not connect to localhost port " + sendPort + ". Trying again in " + msToWait + "ms.");
				try {
					Thread.sleep(msToWait);
				} catch (InterruptedException e1) {/* do nothing */}
			} catch (UnknownHostException e) {
				us.printderp("SocketSender: Unknown host.");
				return false;
			} catch (IOException e) {
				us.printderp("SocketSender: IOException: " + e.getMessage());
				return false;
			}			
		}
		
		BufferedWriter out;
		try {
			out = new BufferedWriter(new OutputStreamWriter(s.getOutputStream()));
			out.write(msg);
			us.printderp("Socket message sent: \"" + msg + "\"");
			out.newLine();
			out.flush();
			out.close();
			s.close();
		} catch (IOException e) {
			us.printderp("SocketSender could not send msg: " + msg  + ". IOException: " + e.getMessage());
			return false;
		}
		return true;
	}
}
