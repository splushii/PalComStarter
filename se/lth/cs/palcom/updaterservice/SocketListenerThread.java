package se.lth.cs.palcom.updaterservice;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

// +----------------------------------------------------------------------------------------------+
// |                          SocketListener Thread                                               |
// +----------------------------------------------------------------------------------------------+
/**
 * A thread class used to receive and buffer Update Protocols commands over TCP sockets.
 * Ugly, but does its job.
 * @author splushii
 *
 */
public class SocketListenerThread extends Thread {
	public static final int WAIT_FOREVER = -1;
	private int listeningPort;
	private LinkedBlockingQueue<String> msgBox;
	private ServerSocket ss;
	private Semaphore wakeLock = new Semaphore(0);
	private boolean halt = false;
	private UpdaterService us;
	
	public SocketListenerThread(UpdaterService us, int port) {
		this.listeningPort = port;
		this.us = us;
		msgBox = new LinkedBlockingQueue<String>();
	}
	
	public void stopThread() {
		if (!halt) {
			halt = true;
			if(ss.isClosed()) {
				wakeLock.release();
			} else {
				closeSocket();				
			}			
		}
	}
	
	public String getMsg() {
		try {
			return msgBox.take();
		} catch (InterruptedException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public String waitForMsg(String msgID, int waitInSeconds) {
		String msg = null;
		long stopTimeMillis = System.currentTimeMillis() + waitInSeconds*1000;
		while(true) {
			try {
				if (waitInSeconds == WAIT_FOREVER) {
					msg = msgBox.take();
				} else {
					long currentTimeMillis = System.currentTimeMillis();
					if (stopTimeMillis < currentTimeMillis) {
						return msg;
					}
					msg = msgBox.poll(stopTimeMillis-currentTimeMillis, TimeUnit.MILLISECONDS);					
				}
				if (msg != null && msg.equals(msgID)) {
					return msg;
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}				
		}
	}
	
	private void addMsg(String msg) {
		try {
			msgBox.put(msg);
		} catch (InterruptedException e) {
			us.printderp("Got interrupted when trying to add msg \"" + msg + "\" to SocketListener's msgBox");
			e.printStackTrace();
		}
	}
	
	public void closeSocket() {
		try {
			ss.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public boolean isSocketClosed() {
		return ss.isClosed();
	}
	
	public void reopenSocket() {
		wakeLock.release();
	}

	@Override
	public void run() {
		us.printderp("SocketListener Thread started. Socket listening on port " + listeningPort);
		try {
			ss = new ServerSocket(listeningPort);
		} catch (IOException e1) {
			e1.printStackTrace();
			us.printderp("SocketListener Thread killed.");
			return;
		}
		Socket sock;
		while (true) {
			try {
				sock = ss.accept();
//				PrintWriter out = new PrintWriter(sock.getOutputStream(), true);
				BufferedReader in = new BufferedReader(new InputStreamReader(sock.getInputStream()));
				String line = null;
				while ((line = in.readLine()) != null) {
					us.printderp("Socket message received \"" + line + "\"");
					addMsg(line);
				}
			} catch (SocketException e) {
				if (halt) {
					us.printderp("SocketListener Thread killed.");
					return;
				}
				us.printderp("SocketException (" + e.getMessage() + ") in SocketListener. Waiting until awoken.");
				try {
					ss.close();
				} catch (IOException e2) {
					us.printderp("Could not close socket in SocketListener.");
				}
				wakeLock.acquireUninterruptibly();
				if (halt) {
					us.printderp("SocketListener Thread killed.");
					return;
				}
				try {
					ss = new ServerSocket(listeningPort);
				} catch (IOException e2) {
					e2.printStackTrace();
					us.printderp("Could not open SocketListener again. Returning");
					break;
				}
			} catch (IOException e) {
				e.printStackTrace();					
			}
		}
		us.printderp("SocketListener Thread killed.");
	}
}