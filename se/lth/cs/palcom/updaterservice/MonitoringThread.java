package se.lth.cs.palcom.updaterservice;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;

import ist.palcom.resource.descriptor.Command;
import ist.palcom.resource.descriptor.DeviceID;
import se.lth.cs.palcom.discovery.DiscoveryManager;
import se.lth.cs.palcom.discovery.Resource;
import se.lth.cs.palcom.discovery.ResourceListener;
import se.lth.cs.palcom.discovery.proxy.PalcomDevice;
import se.lth.cs.palcom.discovery.proxy.implementors.DeviceProxy;
import se.lth.cs.palcom.filesystem.HostFileSystems;
import se.lth.cs.palcom.logging.Logger;
import se.lth.cs.palcom.util.configuration.DeviceList;

/**
 * Monitors specified devices. Uses discovery mechanism to see 
 * if monitored devices are up or down. If they are down, and monitoring
 * is enabled, they are restarted.
 * @author Christian Hernvall
 */
class MonitoringThread extends Thread implements ResourceListener {
	private UpdaterService us;
	private DiscoveryManager dm;
	private SocketListenerThread socketListener;
	private SocketSender socketSender;
	private static final int MAX_SECONDS_WAIT_FOR_DEVICE = 5;
	private static final int RECENTLY_STARTED_WAIT_SEC = 10;
	private ArrayList<MonitoredDevice> monitoredDevices;
	private HashMap<String, String> typeToVersionMap;
	private HashMap<String, Integer> typeToNewMajorVersionMap;
	private Semaphore disableMonitorLock;
	private Semaphore checkListSemaphore;
	private Semaphore doingStuffLock;
	private boolean halt = false;
	private boolean monitoringEnabled = true;
	private Timer timer;
	private ConcurrentLinkedQueue<MonitoredDevice> checkList;
	private LinkedList<MonitoredDevice> newVersionMonitoredDevicesRunning;

	MonitoringThread(UpdaterService us, SocketListenerThread socketListener, SocketSender socketSender) {
		this.us = us;
		this.dm = us.getDevice().getDiscoveryManager();
		this.socketListener = socketListener;
		this.socketSender = socketSender;
		monitoredDevices = new ArrayList<MonitoredDevice>();
		disableMonitorLock = new Semaphore(1);
		checkListSemaphore = new Semaphore(0);
		doingStuffLock = new Semaphore(1);
		timer = new Timer();
		checkList = new ConcurrentLinkedQueue<MonitoredDevice>();
		typeToVersionMap = new HashMap<String, String>();
		typeToNewMajorVersionMap = new HashMap<String, Integer>();
	}
	

	boolean readyForMajorUpdate() {
		int referenceVersion = -1;
		for (String deviceType: typeToVersionMap.keySet()) {
			Integer tmp = typeToNewMajorVersionMap.get(deviceType);
			if (tmp == null) {
				return false;
			} else if (tmp != referenceVersion) {
				if (referenceVersion == -1) { // This is the first major version to compare
					referenceVersion = typeToNewMajorVersionMap.get(deviceType);
				} else {
					return false;
				}
			}
		}
		return true;
	}


	void setNewMajorVersion(String deviceType, int newMajorVersion) {
		typeToNewMajorVersionMap.put(deviceType, newMajorVersion);
	}

	void setCurrentDeviceTypeVersion(String deviceType, String deviceVersion) {
		typeToVersionMap.put(deviceType, deviceVersion);
	}

	List<MonitoredDevice> getMonitoredDevicesOfType(String deviceType) {
		List<MonitoredDevice> mdList = new ArrayList<MonitoredDevice>();
		for (MonitoredDevice d: monitoredDevices)
			if (d.deviceType.equals(deviceType))
				mdList.add(d);
		return mdList;
	}
	
	List<String> getMonitoredDeviceTypes() {
		List<String> deviceTypes = new LinkedList<String>();
		for (MonitoredDevice d: monitoredDevices) {
			if (!deviceTypes.contains(d.deviceType)) {
				deviceTypes.add(d.deviceType);
			}
		}
		return deviceTypes;
	}

	boolean monitorsDeviceType(String deviceType) {
		return typeToVersionMap.containsKey(deviceType);
	}

	String getListOfMonitoredDevices() {
		String list = "";
		int i = 0;
		for (MonitoredDevice d: monitoredDevices) {
			list += "index=" + i++ + " ID=" + d.deviceID + " type=" + d.deviceType + " ";
		}
		return list;
	}

	void stopThread() {
		if (!halt) {
			halt = true;
			enable(); // Nothing bad happens if monitoring is already enabled
			checkListSemaphore.release();				
		}
	}

	void addNewMonitoredDevice(String deviceID, String instanceName, String typeOfDevice, String monitoredDeviceVersion) {
		DeviceID dID = new DeviceID(deviceID);
		PalcomDevice palcomDevice = dm.getDevice(dID);
		palcomDevice.addListener(this);
		typeToVersionMap.put(typeOfDevice, monitoredDeviceVersion);
		MonitoredDevice monitoredDevice = new MonitoredDevice(dID, typeOfDevice, palcomDevice);
		monitoredDevices.add(monitoredDevice);
		us.log("Adding device to monitor:", Logger.CMP_SERVICE, Logger.LEVEL_DEBUG);
		us.log("Name: " + instanceName, Logger.CMP_SERVICE, Logger.LEVEL_DEBUG);
		us.log("ID: " + deviceID, Logger.CMP_SERVICE, Logger.LEVEL_DEBUG);
		us.log("Device type: " + typeOfDevice, Logger.CMP_SERVICE, Logger.LEVEL_DEBUG);
		us.log("", Logger.CMP_SERVICE, Logger.LEVEL_DEBUG);
	}

	/**
	 * Blocking until monitoring thread has stopped.
	 */
	void disable() {
		if (monitoringEnabled) {
			us.log("Disabling monitoring.", Logger.CMP_SERVICE, Logger.LEVEL_INFO);
			disableMonitorLock.acquireUninterruptibly();
			// wait for the monitoring thread to finish what it is doing
			if(!doingStuffLock.tryAcquire()){
				us.log("Waiting for monitoring thread to finish what it is doing...", Logger.CMP_SERVICE, Logger.LEVEL_DEBUG);
				doingStuffLock.acquireUninterruptibly();
				us.log("Monitoring thread has finished doing its stuff. Monitoring is now disabled.", Logger.CMP_SERVICE, Logger.LEVEL_DEBUG);
			}
			doingStuffLock.release();		
			monitoringEnabled = false;
		}
	}
	
	void enable() {
		if (!monitoringEnabled) {
			us.log("Enabling monitoring.", Logger.CMP_SERVICE, Logger.LEVEL_INFO);
			disableMonitorLock.release();
			monitoringEnabled = true;
		}
	}

	void killMonitoredDeviceByIndex(int index, boolean b) {
		if (index >= 0 && index < monitoredDevices.size())
			killMonitoredDevice(monitoredDevices.get(index), b);
	}
	
	MonitoredDevice initiateStageTwo() {
		for (MonitoredDevice d: monitoredDevices) {
			Command cmd = us.getCommand(UpdaterService.COMMAND_IN_INITIATE_STAGE_TWO);
			if(sendCommandToMonitoredDevice(d, cmd)) {
				return d;
			}
		}
		return null;
	}
	
	private boolean sendCommandToMonitoredDevice(MonitoredDevice monitoredDevice, Command cmd) {
		if (monitoredDevice.conn == null){
			us.log("No connection registered to device " + monitoredDevice.deviceID, Logger.CMP_SERVICE, Logger.LEVEL_WARNING);
			return false;
		}
		return us.sendPalComMessage(monitoredDevice.conn, cmd);
	}

	synchronized void killMonitoredDevice(MonitoredDevice monitoredDevice, boolean startGentle) {
		if (startGentle) {
			Command killCmd = us.getCommand(UpdaterService.COMMAND_OUT_KILL);
			if(!sendCommandToMonitoredDevice(monitoredDevice, killCmd)) {
				us.log("Could not send " + killCmd.getID() + " to " + monitoredDevice.deviceID, Logger.CMP_SERVICE, Logger.LEVEL_WARNING);
			}
		}
		if(monitoredDevice.p != null) {
			us.log("Trying to kill monitored device forcibly...", Logger.CMP_SERVICE, Logger.LEVEL_DEBUG);
			monitoredDevice.p.destroyForcibly();
			try {
				monitoredDevice.p.waitFor();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			us.log("Device killed.", Logger.CMP_SERVICE, Logger.LEVEL_DEBUG);
			monitoredDevice.p = null;
		}
	}
	
	void killAllMonitoredDevices(boolean startGentle) {
		for (MonitoredDevice d: monitoredDevices)
			killMonitoredDevice(d, startGentle);
	}

	void startMonitoredDeviceByIndex(int index) {
		if (index >= 0 && index < monitoredDevices.size()) {
			MonitoredDevice d = monitoredDevices.get(index);
			if (startMonitoredDevice(d))
				performMonitoredDeviceStartupCheck(d);
		}
	}
	
	synchronized boolean startMonitoredDevice(MonitoredDevice monitoredDevice) {
		String pathToJar = getMonitoredDevicePathToJar(monitoredDevice);
		return startMonitoredDeviceHelper(monitoredDevice, pathToJar);
	}

	boolean startNewVersionMonitoredDevice(MonitoredDevice monitoredDevice, String version) {
		if (newVersionMonitoredDevicesRunning == null) {
			newVersionMonitoredDevicesRunning = new LinkedList<MonitoredDevice>();
		}
		newVersionMonitoredDevicesRunning.add(monitoredDevice);
		String pathToJar;
		try {
			pathToJar = DeviceList.getConfFolder(monitoredDevice.deviceType).getNativeURL().replace("file:", "");
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
		pathToJar += monitoredDevice.deviceType + "-" + version + ".jar";
		return startMonitoredDeviceHelper(monitoredDevice, pathToJar);
	}
	
	void killAllNewVersionMonitoredDevices() {
		for (MonitoredDevice newMonitoredDevice: newVersionMonitoredDevicesRunning) {
			killMonitoredDevice(newMonitoredDevice, false);
		}
	}
	
	boolean startMonitoredDeviceHelper(MonitoredDevice monitoredDevice, String pathToJar) {
		String pathToFS = HostFileSystems.getUnixStylePathToFilesystemRoot().replace("/PalcomFilesystem", "");
		String[] arguments = {"java", "-jar", pathToJar, "-x", monitoredDevice.deviceID.getID(), "-f", pathToFS};
		String msg = "Starting monitored device with:";
		for(String s: arguments)
			msg += " " + s;
		us.log(msg, Logger.CMP_SERVICE, Logger.LEVEL_DEBUG);
		ProcessBuilder pb = new ProcessBuilder(arguments);
		pb.inheritIO();
		try {
			monitoredDevice.p = pb.start();
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
		monitoredDevice.recentlyStartedDelay = System.currentTimeMillis() + RECENTLY_STARTED_WAIT_SEC*1000;
		return true;
	}
	
	
	private String getMonitoredDevicePathToJar(MonitoredDevice monitoredDevice) {
		String version = typeToVersionMap.get(monitoredDevice.deviceType);
		String pathToJar;
		try {
			pathToJar = DeviceList.getConfFolder(monitoredDevice.deviceType).getNativeURL().replace("file:", "");
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
		pathToJar += monitoredDevice.deviceType + "-" + version + ".jar";
		return pathToJar;
	}


	void restartAllMonitoredDevices() {
		for (MonitoredDevice d: monitoredDevices)
			restartMonitoredDevice(d);
	}
	
	void restartMonitoredDeviceByIndex(int index) {
		if (index >= 0 && index < monitoredDevices.size())
			restartMonitoredDevice(monitoredDevices.get(index));
	}
	
	void restartMonitoredDevice(MonitoredDevice d) {
		// Kill first, in case it is a zombie. It was not found by PalCom discovery so there is
//		// no idea to try to kill it via PalCom commands. Kill it with brute force.
		killMonitoredDevice(d, false);
		
		if (!startMonitoredDevice(d)) {
			us.log("Could not start monitored device: " + d.deviceID + ". Will try again in " + RECENTLY_STARTED_WAIT_SEC + "s.", Logger.CMP_SERVICE, Logger.LEVEL_WARNING);
			killMonitoredDevice(d, false);
			if (!checkList.contains(d)) {
				checkList.add(d);
				timer.schedule(new TimerTask() {
					@Override
					public void run() {
						checkListSemaphore.release();
					}
				}, RECENTLY_STARTED_WAIT_SEC*1000);					
			}
			return;
		}
		if(!performMonitoredDeviceStartupCheck(d)) {
			us.log("Monitored device " + d.deviceID + " failed startup check. Will try again in " + RECENTLY_STARTED_WAIT_SEC + "s.", Logger.CMP_SERVICE, Logger.LEVEL_WARNING);
			if (!checkList.contains(d)) {
				checkList.add(d);
				timer.schedule(new TimerTask() {
					@Override
					public void run() {
						checkListSemaphore.release();
					}
				}, RECENTLY_STARTED_WAIT_SEC*1000);					
			}
			return;
		} else {
			us.log("Child started and startup check finished.", Logger.CMP_SERVICE, Logger.LEVEL_DEBUG);
		}
	}
	
	private void checkMonitoredDevice(MonitoredDevice d) {
		PalcomDevice pd = d.palcomDevice;
		if(!pd.isReady()) {
			us.log("Could not find " + d.deviceID.getID() + " on network.", Logger.CMP_SERVICE, Logger.LEVEL_INFO);
			long timeDiff = d.recentlyStartedDelay - System.currentTimeMillis();
			if (timeDiff > 0) {
				us.log("Recently started " + d.deviceID.getID() + ". Waiting " + timeDiff + "ms before trying to start it again.", Logger.CMP_SERVICE, Logger.LEVEL_INFO);
				// Schedule the check if it is not already scheduled
				if (!checkList.contains(d)) {
					checkList.add(d);
					timer.schedule(new TimerTask() {
						@Override
						public void run() {
							checkListSemaphore.release();
						}
					}, RECENTLY_STARTED_WAIT_SEC*1000);					
				}
			} else {
				us.log("It will be restarted.", Logger.CMP_SERVICE, Logger.LEVEL_INFO);
				restartMonitoredDevice(d);					
			}
		} else {
			us.log("Device " + pd.getDeviceID().getID() + " is up and running. It does not need to be restarted.", Logger.CMP_SERVICE, Logger.LEVEL_DEBUG);
		}
	}
	
	private void checkAllMonitoredDevices() {
		for (MonitoredDevice d: monitoredDevices) {
			checkMonitoredDevice(d);
		}
	}
	
	private boolean performMonitoredDeviceStartupCheck(MonitoredDevice d) {
		if (socketSender == null) {
			System.err.println("DERP IT IS NULL");
		}
		if (!socketSender.sendMsg(UpdaterService.UPDATE_PROTOCOL_CHECK_SOCKET, MAX_SECONDS_WAIT_FOR_DEVICE)) {
			us.log("Send msg timeout: Could not send msg to monitored device. Shutting down device again.", Logger.CMP_SERVICE, Logger.LEVEL_ERROR);
			killMonitoredDevice(d, false);
			return false;
		}
		if (socketListener.waitForMsg(UpdaterService.UPDATE_PROTOCOL_CHECK_SOCKET_CONFIRM, MAX_SECONDS_WAIT_FOR_DEVICE) == null) {
			us.log("Wait for msg timeout: No socket response from monitored device. Shutting down device again.", Logger.CMP_SERVICE, Logger.LEVEL_ERROR);
			killMonitoredDevice(d, false);
			return false;
		}
		if (!socketSender.sendMsg(UpdaterService.UPDATE_PROTOCOL_FINISH_DEVICE_STARTUP_CHECK, MAX_SECONDS_WAIT_FOR_DEVICE)) {
			us.log("Send msg timeout: Could not send msg to monitored device. Shutting down device again.", Logger.CMP_SERVICE, Logger.LEVEL_ERROR);
			killMonitoredDevice(d, false);
			return false;
		}
		if (socketListener.waitForMsg(UpdaterService.UPDATE_PROTOCOL_FINISH_DEVICE_STARTUP_CHECK_ACK, MAX_SECONDS_WAIT_FOR_DEVICE) == null) {
			us.log("Wait for msg timeout: No socket response from monitored device. Shutting down device again.", Logger.CMP_SERVICE, Logger.LEVEL_ERROR);
			killMonitoredDevice(d, false);
			return false;
		}
		us.log("Startup check finished.", Logger.CMP_SERVICE, Logger.LEVEL_DEBUG);
		return true;
	}

	@Override
	public void run() {
		us.log("Monitoring Thread started. Doing startup check.", Logger.CMP_SERVICE, Logger.LEVEL_DEBUG);
		doingStuffLock.acquireUninterruptibly();
		checkAllMonitoredDevices();
		doingStuffLock.release();
		while(true) {	
			try {
				checkListSemaphore.acquire(); // wait for something to check in the checklist
			} catch (InterruptedException e1) {/* do nothing */}
			if(!disableMonitorLock.tryAcquire()){ // there is something to check, but monitoring may be disabled
				us.log("Monitoring is temporarily disabled.", Logger.CMP_SERVICE, Logger.LEVEL_INFO);
				disableMonitorLock.acquireUninterruptibly();
				us.log("Monitoring is enabled.", Logger.CMP_SERVICE, Logger.LEVEL_INFO);
			}
			disableMonitorLock.release();
			if(halt) {
				us.log("Monitoring Thread stopped.", Logger.CMP_SERVICE, Logger.LEVEL_DEBUG);
				return;
			}
			doingStuffLock.acquireUninterruptibly();
			// BEGIN: Stuff to be done
			MonitoredDevice md = checkList.poll();
			checkMonitoredDevice(md);
			// END: Stuff to be done
			doingStuffLock.release();	
		}
	}
	
	@Override
	public void unavailable(Resource resource) {
		if (resource instanceof DeviceProxy) {
			DeviceProxy dp = (DeviceProxy) resource;
			us.log("Unvailable: " + dp.getDeviceID().getID(), Logger.CMP_SERVICE, Logger.LEVEL_DEBUG);
			for (MonitoredDevice md: monitoredDevices) {
				if (dp.getDeviceID().getID().equals(md.deviceID.getID())) {
					md.conn = null;
					if (!checkList.contains(md)) {
						checkList.add(md);
						checkListSemaphore.release();							
					}
					break;
				}
			}
		} else {
			us.log("Unavailable: " + resource.toString(), Logger.CMP_SERVICE, Logger.LEVEL_DEBUG);				
		}
	}

	@Override
	public void available(Resource resource) {
		if (resource instanceof DeviceProxy) {
			DeviceProxy dp = (DeviceProxy) resource;
			us.log("Available: " + dp.getDeviceID().getID(), Logger.CMP_SERVICE, Logger.LEVEL_DEBUG);
			for (MonitoredDevice md: monitoredDevices) {
				if (dp.getDeviceID().getID().equals(md.deviceID.getID())) {
					md.conn = us.getWritableConnectionToService(md.deviceID, UpdaterService.SERVICE_NAME, MAX_SECONDS_WAIT_FOR_DEVICE);
					break;
				}
			}
		} else {
			us.log("Available: " + resource.toString(), Logger.CMP_SERVICE, Logger.LEVEL_DEBUG);				
		}
	}
	@Override
	public void resourceChanged(Resource resource) { /* We do not care */}
}