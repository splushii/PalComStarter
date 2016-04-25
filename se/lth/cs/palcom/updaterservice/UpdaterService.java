package se.lth.cs.palcom.updaterservice;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import ist.palcom.resource.descriptor.Command;
import ist.palcom.resource.descriptor.DeviceID;
import ist.palcom.resource.descriptor.Group;
import ist.palcom.resource.descriptor.PRDService;
import ist.palcom.resource.descriptor.ServiceID;
import se.lth.cs.palcom.common.NoSuchDeviceException;
import se.lth.cs.palcom.common.PalComVersion;
import se.lth.cs.palcom.common.TreeUpdateException;
import se.lth.cs.palcom.communication.connection.Connection;
import se.lth.cs.palcom.communication.connection.Readable;
import se.lth.cs.palcom.communication.connection.Writable;
import se.lth.cs.palcom.device.AbstractDevice;
import se.lth.cs.palcom.device.DeviceProperties;
import se.lth.cs.palcom.discovery.ResourceException;
import se.lth.cs.palcom.discovery.proxy.PalcomDevice;
import se.lth.cs.palcom.discovery.proxy.PalcomService;
import se.lth.cs.palcom.discovery.proxy.PalcomServiceList;
import se.lth.cs.palcom.filesystem.HostFileSystems;
import se.lth.cs.palcom.logging.Logger;
import se.lth.cs.palcom.palcomstarter.PalComStarter;
import se.lth.cs.palcom.service.AbstractSimpleService;
import se.lth.cs.palcom.service.ServiceTools;
import se.lth.cs.palcom.service.command.CommandServiceProtocol;
import se.lth.cs.palcom.service.distribution.UnicastDistribution;
import se.lth.cs.palcom.updatedistributionservice.UpdateDistributionService;

/** 
 * Service that can either act by monitoring or by being monitored. When monitoring, can also update all its monitored 
 * devices by communicating with and getting updates from an {@link UpdateDistributionService}. 
 * @author Christian Hernvall
 *
 */
public class UpdaterService extends AbstractSimpleService {

	public static final DeviceID CREATOR = new DeviceID("X:mojo");
	public static final ServiceID SERVICE_VERSION = new ServiceID(CREATOR, "UP1.0.0", CREATOR, "UP1.0.0");
	public static final String SERVICE_NAME = "UpdaterService";

	public static final String COMMAND_IN_UPDATE_DEVICE_TYPES = "update single device type";
	static final String COMMAND_IN_STOP_MONITORED_DEVICES = "stop all monitored devices";
	public static final String COMMAND_IN_UPDATE_DATA = "updateData";
	public static final String COMMAND_IN_CHECK_UPDATE_SERVER_CONFIRM = "I hear you!";
	static final String COMMAND_IN_ABORT_UPDATE = "abort update!";
	static final String COMMAND_IN_KILL = "kill";
	static final String COMMAND_IN_INITIATE_STAGE_TWO = "initiate updating stage two";

	static final String COMMAND_IN_DISABLE_MONITORING = "disable monitor";
	static final String COMMAND_IN_ENABLE_MONITORING = "enable monitor";
	static final String COMMAND_IN_LIST_MONITORED_DEVICES = "list all monitored devices";
	static final String COMMAND_IN_KILL_DEVICE_BY_INDEX = "kill device by index";
	static final String COMMAND_IN_START_DEVICE_BY_INDEX = "start device by index";
	static final String COMMAND_IN_RESTART_DEVICE_BY_INDEX = "restart device by index";
	static final String COMMAND_IN_RESET_UPDATE_ABORTED_COUNTER = "reset update aborted counter";

	public static final String COMMAND_OUT_UPDATE_CONTENT_REQUEST = "gief the jar!";
	public static final String COMMAND_OUT_CHECK_UPDATE_SERVER = "do you hear me?";
	static final String COMMAND_OUT_KILL = COMMAND_IN_KILL;
	public static final String COMMAND_OUT_CHECK_LATEST_VERSION = "latest version?";
	static final String COMMAND_OUT_LIST_MONITORED_DEVICES = "list of all monitored devices";
	public static final String COMMAND_OUT_BENCHMARK_END = "benchmark end";
	static final String COMMAND_OUT_INITIATE_STAGE_TWO = COMMAND_IN_INITIATE_STAGE_TWO;

	public static final String PARAM_VALUE_SEPARATOR = ",,,";
	public static final String PARAM_NO_ENTRY = "no entry";
	public static final String PARAM_IMPLEMENTATION = "implementation";
	public static final String PARAM_VERSION = "version";
	public static final String PARAM_DEVICE_TYPE = "device type";
	public static final String PARAM_UPDATE_CONTENT = "jar content";
	static final String PARAM_SERVICEINSTANCEID = "serviceInstanceID";
	static final String PARAM_MONITORED_DEVICES = "monitored devices";
	static final String PARAM_MONITORED_DEVICE_INDEX = "monitored device index";

	static final String NAMESPACE_UPDATERSERVICE_MONITORED_DEVICE_NAMES = "monitoredDeviceNames";
	static final String NAMESPACE_MONITORED_DEVICE = "monitoredDevice-";
	static final String NAMESPACE_UPDATERSERVICE_DEVICE_TYPE_VERSION = "deviceTypeVersion";
	static final String NAMESPACE_UPDATERSERVICE_GENERAL = "general";

	static final String KEY_MONITORED_DEVICE_ID = "ID";
	static final String KEY_MONITORED_DEVICE_TYPE = "type";
	static final String KEY_UPDATE_SERVER_DEVICE_ID = "updateServerDeviceID";
	static final String KEY_UPDATE_ABORTED = "updateAborted";

	private static final String PROPERTY_MONITORED_DEVICE_ENABLED = "enabled";

	static final String UPDATE_PROTOCOL_KILL = "kill";
	static final String UPDATE_PROTOCOL_KILL_ACK = "kill ack";
	static final String UPDATE_PROTOCOL_ABORT = "abort!";
	static final String UPDATE_PROTOCOL_CHECK_SOCKET = "socket working?";
	static final String UPDATE_PROTOCOL_CHECK_SOCKET_CONFIRM = "socket working!";
	static final String UPDATE_PROTOCOL_CHECK_UPDATE_SERVER = "update server hear you?";
	static final String UPDATE_PROTOCOL_CHECK_UPDATE_SERVER_CONFIRM = "update server hear me!";
	static final String UPDATE_PROTOCOL_FINISH_DEVICE_STARTUP_CHECK = "finish device startup check";
	static final String UPDATE_PROTOCOL_FINISH_DEVICE_STARTUP_CHECK_ACK = "finish device startup check ACK";
	static final String UPDATE_PROTOCOL_STAGE_TWO = "update stage two";
	static final String UPDATE_PROTOCOL_FINISH_STAGE_TWO = "finish stage two";
	static final String UPDATE_PROTOCOL_FINISH_STAGE_TWO_ACK = "finish stage two ACK";

	static final String MSG_SHUT_DOWN_THREAD = "shut down thread";

	static final String PALCOMSTARTER_DEVICE_TYPE = "PalComStarter";

	static final int UPDATE_ABORTED_DELAY_SECONDS = 30;
	static final int UPDATE_ABORTED_MANY_TIMES = 3;
	static final int UPDATE_ABORTED_MANY_TIMES_DELAY_SECONDS = 24*60*60;
	static final int PALCOMSTARTER_SOCKET_PORT = 13370;
	static final int MONITORED_DEVICE_SOCKET_PORT = 13371;

	enum UpdateState {
		NONE, UPDATING_INITIAL, UPDATING_WAITING_FOR_JAR, UPDATING_KILLING_CURRENT, UPDATING_STARTING_NEW, UPDATING_FALLBACK_TIMER_CHECK_SOCKET, UPDATING_STAGE_TWO, STARTUP, UPDATING_FALLBACK_TIMER_CHECK_UPDATE_SERVER, UPDATING_SENDING_JAR, UPDATING_DONT_DISTURB, UPDATING_STAGE_THREE,
	}

	
	private UpdateState updateState = UpdateState.NONE;

	private MonitoringThread monitor;
	private LinkedBlockingQueue<Command> commandBuffer;
	private SocketListenerThread socketListener;
	private SocketSender socketSender;
	private UpdateServerConnectionListener updateServerConnectionListener;
	private boolean isMonitor = false;
	private boolean continueUpdateStageThree = false;
	String updateServerDeviceID;
	DeviceProperties monitoringProperties;
	Integer updateAborted = 0;
	Long updateAbortedDelay = 0L;

	public UpdaterService(AbstractDevice container) {
		this(container, ServiceTools.getNextInstance(SERVICE_VERSION));
	}
	
	public UpdaterService(AbstractDevice container, boolean continueUpdateStageThree) {
		super(container, SERVICE_VERSION, "P1", "v0.0.1", "UpdaterService",
				ServiceTools.getNextInstance(SERVICE_VERSION), "Updates PalCom devices",
				new UnicastDistribution(true));
		this.continueUpdateStageThree = continueUpdateStageThree;
		log("Continue updating stage three: " + continueUpdateStageThree, Logger.CMP_SERVICE, Logger.LEVEL_DEBUG);
		configureService();
	}
	
	public UpdaterService(AbstractDevice container, String instance) {
		super(container, SERVICE_VERSION, "P1", "v0.0.1", "UpdaterService",
				instance, "Updates PalCom devices",
				new UnicastDistribution(true));
		configureService();
	}
	
	private void configureService() {
		if (container instanceof PalComStarter)
			isMonitor = true;
		if (isMonitor) {
			try {
				monitoringProperties = new DeviceProperties(new DeviceID("monitoring"), HostFileSystems.getGlobalRoot(), null, "Monitoring properties. Generated " + new Date());
				String[] monitoredDeviceNames = monitoringProperties.getKeys(NAMESPACE_UPDATERSERVICE_MONITORED_DEVICE_NAMES);
				socketListener = new SocketListenerThread(this, PALCOMSTARTER_SOCKET_PORT);
				socketSender = new SocketSender(this, MONITORED_DEVICE_SOCKET_PORT);
				monitor = new MonitoringThread(this, socketListener, socketSender);
				for (String deviceName: monitoredDeviceNames) {
					if (!monitoringProperties.getProperty(NAMESPACE_UPDATERSERVICE_MONITORED_DEVICE_NAMES, deviceName).equals(PROPERTY_MONITORED_DEVICE_ENABLED)){
						continue;
					}
					String deviceSpecificNamespace = NAMESPACE_MONITORED_DEVICE + deviceName;
					String monitoredDeviceID = monitoringProperties.getProperty(deviceSpecificNamespace, KEY_MONITORED_DEVICE_ID);
					String monitoredDeviceType = monitoringProperties.getProperty(deviceSpecificNamespace, KEY_MONITORED_DEVICE_TYPE);
					if (monitoredDeviceType == null) {
						log("ERROR: Device type is not specified in the configuration: " + deviceSpecificNamespace + "@" + KEY_MONITORED_DEVICE_TYPE, Logger.CMP_SERVICE, Logger.LEVEL_WARNING);
						log("Will not monitor device with name: " + deviceName, Logger.CMP_SERVICE, Logger.LEVEL_WARNING);
						continue;
					}
					String monitoredDeviceVersion = monitoringProperties.getProperty(NAMESPACE_UPDATERSERVICE_DEVICE_TYPE_VERSION, monitoredDeviceType);
					if (monitoredDeviceVersion == null) {
						log("ERROR: Device version is not specified in the configuration: " + deviceSpecificNamespace + "@" + KEY_MONITORED_DEVICE_TYPE, Logger.CMP_SERVICE, Logger.LEVEL_WARNING);
						log("Will not monitor device with name: " + deviceName, Logger.CMP_SERVICE, Logger.LEVEL_WARNING);
						continue;
					}
					if (monitoredDeviceID == null) {
						// The device is lacking its device ID which means that it is not installed yet.
						/**TODO Generate a new device if it is missing its device ID. 
						 * This should probably be done using another thread or another service
						 * because it includes downloading an executable and a configuration for
						 * the new device, followed by the installation.
						 */
						log("ERROR: Device ID is not specified in the configuration: " + deviceSpecificNamespace + "@" + KEY_MONITORED_DEVICE_TYPE, Logger.CMP_SERVICE, Logger.LEVEL_WARNING);
						log("Will not monitor device with name: " + deviceName, Logger.CMP_SERVICE, Logger.LEVEL_WARNING);
						continue;
					}
					monitor.addNewMonitoredDevice(monitoredDeviceID, deviceName, monitoredDeviceType, monitoredDeviceVersion);
				}
				updateServerDeviceID = monitoringProperties.getProperty(NAMESPACE_UPDATERSERVICE_GENERAL, KEY_UPDATE_SERVER_DEVICE_ID);
				if (updateServerDeviceID == null) {
					log("UpdateServer device ID is not set in configuration: " + NAMESPACE_UPDATERSERVICE_GENERAL + "@" + KEY_UPDATE_SERVER_DEVICE_ID, Logger.CMP_SERVICE, Logger.LEVEL_WARNING);
					log("Will not be able to communicate with or receive updates from Update Server.", Logger.CMP_SERVICE, Logger.LEVEL_WARNING);
				}
				updateServerConnectionListener = new UpdateServerConnectionListener(this, monitor);
			} catch (IOException e) {
				log("Could not access monitoring.properties. UpdateServer and monitored devices unknown. Reason: ", Logger.CMP_SERVICE, Logger.LEVEL_WARNING);
				e.printStackTrace();
			}
		} else { // This means that this service is running on a monitored device, not a PalComStarter. Reverse the ports.
			socketListener = new SocketListenerThread(this, MONITORED_DEVICE_SOCKET_PORT);
			socketSender = new SocketSender(this, PALCOMSTARTER_SOCKET_PORT);			
		}
		
		commandBuffer = new LinkedBlockingQueue<Command>();

		CommandServiceProtocol sp = getProtocolHandler();

		// General UpdaterService commands
		Command killInCmd = new Command(COMMAND_IN_KILL, "Kill device", Command.DIRECTION_IN);
		
		Command checkUpdateServerCmd = new Command(COMMAND_OUT_CHECK_UPDATE_SERVER, "Ping update server to test connection", Command.DIRECTION_OUT);
		sp.addCommand(checkUpdateServerCmd);
		
		Command checkUpdateServerConfirmCmd = new Command(COMMAND_IN_CHECK_UPDATE_SERVER_CONFIRM, "Confirm the \"do you hear me?\" request", Command.DIRECTION_IN);
		
		Command abortUpdateCmd = new Command(COMMAND_IN_ABORT_UPDATE, "Abort update", Command.DIRECTION_IN);

		Group automaticCmdGroup = new Group("automaticGroup", "Automatic commands that is used during the update process. Do not touch unless you need to do something manually!");

		// Monitor specific commands
		if(isMonitor) {
			Command updateCmd = new Command(COMMAND_IN_UPDATE_DEVICE_TYPES, "Starts update procedure.", Command.DIRECTION_IN);
			updateCmd.addParam(PARAM_DEVICE_TYPE, "text/plain");
			updateCmd.addParam(PARAM_VERSION, "text/plain");

			Command killAllMonitoredDevicesCmd = new Command(COMMAND_IN_STOP_MONITORED_DEVICES, "Kill the monitored devices.", Command.DIRECTION_IN);

			Command updateDataCmd = new Command(COMMAND_IN_UPDATE_DATA, "Update data for the updating process", Command.DIRECTION_IN);
			updateDataCmd.addParam(PARAM_VERSION, "text/plain");
			updateDataCmd.addParam(PARAM_UPDATE_CONTENT, "application/x-jar");
			
			Command killOutCmd = new Command(COMMAND_OUT_KILL, "kill", Command.DIRECTION_OUT);
			sp.addCommand(killOutCmd);
			
			Command updateContentRequestCmd = new Command(COMMAND_OUT_UPDATE_CONTENT_REQUEST, "Request update content from update server.", Command.DIRECTION_OUT);
			updateContentRequestCmd.addParam(PARAM_DEVICE_TYPE, "text/plain");
			updateContentRequestCmd.addParam(PARAM_VERSION, "text/plain");
			sp.addCommand(updateContentRequestCmd);
			
			Command checkLatestVersionCmd = new Command(COMMAND_OUT_CHECK_LATEST_VERSION, "Request latest version info from Update Server.", Command.DIRECTION_OUT);
			checkLatestVersionCmd.addParam(PARAM_DEVICE_TYPE, "text/plain");
			sp.addCommand(checkLatestVersionCmd);
			
			Command initiateStageTwoCmdOut = new Command(COMMAND_OUT_INITIATE_STAGE_TWO, "Initiates updating stage two.", Command.DIRECTION_OUT);
			sp.addCommand(initiateStageTwoCmdOut);

			Command disableMonitoringCmd = new Command(COMMAND_IN_DISABLE_MONITORING, "Disable monitoring of devices.", Command.DIRECTION_IN);

			Command enableMonitoringCmd = new Command(COMMAND_IN_ENABLE_MONITORING, "Enable monitoring of devices.", Command.DIRECTION_IN);
			
			Command listMonitoredDevicesCmd = new Command(COMMAND_IN_LIST_MONITORED_DEVICES, "Lists all monitored devices.", Command.DIRECTION_IN);
			
			Command listMonitoredDevicesReplyCmd = new Command(COMMAND_OUT_LIST_MONITORED_DEVICES, "Reply with all monitored devices.", Command.DIRECTION_OUT);
			listMonitoredDevicesReplyCmd.addParam(PARAM_MONITORED_DEVICES, "text/plain");
			sp.addCommand(listMonitoredDevicesReplyCmd);
			
			Command killSingleDeviceCmd = new Command(COMMAND_IN_KILL_DEVICE_BY_INDEX, "Stop a single device by identified by index.", Command.DIRECTION_IN);
			killSingleDeviceCmd.addParam(PARAM_MONITORED_DEVICE_INDEX, "text/plain");
			
			Command startSingleDeviceCmd = new Command(COMMAND_IN_START_DEVICE_BY_INDEX, "Start a single device by identified by index.", Command.DIRECTION_IN);
			startSingleDeviceCmd.addParam(PARAM_MONITORED_DEVICE_INDEX, "text/plain");
			
			Command restartSingleDeviceCmd = new Command(COMMAND_IN_RESTART_DEVICE_BY_INDEX, "Restart a single device by identified by index.", Command.DIRECTION_IN);
			restartSingleDeviceCmd.addParam(PARAM_MONITORED_DEVICE_INDEX, "text/plain");
			
			Command resetUpdateAbortedCounterCmd = new Command(COMMAND_IN_RESET_UPDATE_ABORTED_COUNTER, "Resets the update aborted counter, so that we can try to update again.", Command.DIRECTION_IN);
			
			Group managementCmdGroup = new Group("managementGroup", "Manual management commands.");
			managementCmdGroup.addCommand(enableMonitoringCmd);
			managementCmdGroup.addCommand(disableMonitoringCmd);
			managementCmdGroup.addCommand(killAllMonitoredDevicesCmd);
			managementCmdGroup.addCommand(listMonitoredDevicesCmd);
			managementCmdGroup.addCommand(killSingleDeviceCmd);
			managementCmdGroup.addCommand(startSingleDeviceCmd);
			managementCmdGroup.addCommand(restartSingleDeviceCmd);
			managementCmdGroup.addCommand(resetUpdateAbortedCounterCmd);
			sp.addGroup(managementCmdGroup);
			
			automaticCmdGroup.addCommand(updateCmd);
			automaticCmdGroup.addCommand(updateDataCmd);
			
			Command benchmarkEndCmd = new Command(COMMAND_OUT_BENCHMARK_END, "benchmark end", Command.DIRECTION_OUT);
			sp.addCommand(benchmarkEndCmd);
		} else {
			// Monitored device specific commands
			Command initiateStageTwoCmdIn = new Command(COMMAND_IN_INITIATE_STAGE_TWO, "Initiates updating stage two.", Command.DIRECTION_IN);
			automaticCmdGroup.addCommand(initiateStageTwoCmdIn);
		}
		automaticCmdGroup.addCommand(abortUpdateCmd);
		automaticCmdGroup.addCommand(killInCmd);
		automaticCmdGroup.addCommand(checkUpdateServerConfirmCmd);
		sp.addGroup(automaticCmdGroup);
	}
	
	public void start() {
		socketListener.start();
		if (isMonitor) {
			log("UpdaterService is running as a monitoring device / PalComStarter.", Logger.CMP_SERVICE, Logger.LEVEL_DEBUG);
			new PalComStarterStartThread().start();
		} else {
			log("UpdaterService is running as a monitored device.", Logger.CMP_SERVICE, Logger.LEVEL_DEBUG);
			new MonitoredDeviceStartThread().start();
		}
	}
	
	void stopDevice() {
		log("Stopping device.", Logger.CMP_SERVICE, Logger.LEVEL_INFO);
		stopUpdaterService();
		System.exit(0);
	}
	
	private void stopUpdaterService() {
		stop();
	}
	
	private void stopHelperThreads() {
		socketListener.stopThread();
		try {
			socketListener.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		if(isMonitor) {
			monitor.stopThread();
			try {
				monitor.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	public void stop() {
		stopHelperThreads();
		super.stop();
	}
	
	public void setAsFullyOperational() {
		setStatus(PRDService.FULLY_OPERATIONAL);
		super.start();
	}

	protected void connectionOpened(Connection conn) {
	}

	protected void connectionClosed(Connection conn) {
	}

	public static String toUTF8String(byte[] data) {
		try {
			return new String(data, "UTF-8");			
		} catch (UnsupportedEncodingException e) {
			System.err.println("UTF-8 must be supported!");
			System.exit(0);
		}
		return null;
	}
	
	protected void invoked(Readable conn, Command command) {
		if (conn instanceof Writable) {
			if (command.getID().equals(COMMAND_IN_KILL)) {
				stopDevice();
			}
			if (command.getID().equals(COMMAND_IN_ABORT_UPDATE)) {
				addCommandToBuffer(command);
				return;
			}
			switch (updateState) {
			case STARTUP:
				if (command.getID().equals(COMMAND_IN_CHECK_UPDATE_SERVER_CONFIRM)) {
					addCommandToBuffer(command);
				}
				break;
			case NONE:
				if (isMonitor) {
					if (command.getID().equals(COMMAND_IN_UPDATE_DEVICE_TYPES)) {
						updateState = UpdateState.UPDATING_INITIAL;
						// We are now in the updating state. Commands that could interrupt the procedure are ignored
						log("Got new update from UpdateServer", Logger.CMP_SERVICE, Logger.LEVEL_DEBUG);
						String deviceTypes = toUTF8String(command.findParam(PARAM_DEVICE_TYPE).getData());
						String newVersions = toUTF8String(command.findParam(PARAM_VERSION).getData());
						if (deviceTypes == null) {
							log("Device types is null. Will not update.", Logger.CMP_SERVICE, Logger.LEVEL_ERROR);
						} else if (newVersions == null) {
							log("Versions is null. Will not update.", Logger.CMP_SERVICE, Logger.LEVEL_ERROR);
						} else {
							String[] splitDeviceTypes = deviceTypes.split(PARAM_VALUE_SEPARATOR);
							String[] splitNewVersions = newVersions.split(PARAM_VALUE_SEPARATOR);
							if (splitDeviceTypes.length < 1) {
								log("There are no device types. Will not update.", Logger.CMP_SERVICE, Logger.LEVEL_ERROR);
							} else if (splitNewVersions.length < 1) {
								log("There are no versions. Will not update.", Logger.CMP_SERVICE, Logger.LEVEL_ERROR);
							} else {
								new UpdateStageOneThread(this, monitor, socketListener, socketSender, (Writable) conn, splitDeviceTypes, splitNewVersions).start();														
							}
						}
					} else if (command.getID().equals(COMMAND_IN_STOP_MONITORED_DEVICES)) {
						monitor.killAllMonitoredDevices(true);
					} else if (command.getID().equals(COMMAND_IN_DISABLE_MONITORING)) {
						monitor.disable();
					} else if (command.getID().equals(COMMAND_IN_ENABLE_MONITORING)) {
						monitor.enable();
					} else if (command.getID().equals(COMMAND_IN_LIST_MONITORED_DEVICES)) {
						Command reply = getProtocolHandler().findCommand(COMMAND_OUT_LIST_MONITORED_DEVICES);
						String listOfMonitoredDevices = monitor.getListOfMonitoredDevices();
						reply.findParam(PARAM_MONITORED_DEVICES).setData(listOfMonitoredDevices.getBytes());
						sendTo((Writable) conn, reply);
					} else if (command.getID().equals(COMMAND_IN_KILL_DEVICE_BY_INDEX)) {
						int index = Integer.valueOf(toUTF8String(command.findParam(PARAM_MONITORED_DEVICE_INDEX).getData()));
						monitor.killMonitoredDeviceByIndex(index, true);
					} else if (command.getID().equals(COMMAND_IN_START_DEVICE_BY_INDEX)) {
						int index = Integer.valueOf(toUTF8String(command.findParam(PARAM_MONITORED_DEVICE_INDEX).getData()));
						monitor.startMonitoredDeviceByIndex(index);
					} else if (command.getID().equals(COMMAND_IN_RESTART_DEVICE_BY_INDEX)) {
						int index = Integer.valueOf(toUTF8String(command.findParam(PARAM_MONITORED_DEVICE_INDEX).getData()));
						monitor.restartMonitoredDeviceByIndex(index);
					} else if (command.getID().equals(COMMAND_IN_RESET_UPDATE_ABORTED_COUNTER)) {
						updateAborted = 0;
						monitoringProperties.getProperty(NAMESPACE_UPDATERSERVICE_GENERAL, KEY_UPDATE_ABORTED);
						log("\"Update Aborted\"-counter reset.", Logger.CMP_SERVICE, Logger.LEVEL_DEBUG);
					} else {
						log("Received unknown command: " + command.getID(), Logger.CMP_SERVICE, Logger.LEVEL_WARNING);
					}
				} else { // is not monitor
					if (command.getID().equals(COMMAND_IN_INITIATE_STAGE_TWO)) {
						updateState = UpdateState.UPDATING_STAGE_TWO;
						socketListener.reopenSocket();
						new UpdateStageTwoThread(this, socketListener, socketSender).start();
					} else {
						log("Received unknown command: " + command.getID(), Logger.CMP_SERVICE, Logger.LEVEL_DEBUG);
					}				
				}
				break;
			case UPDATING_WAITING_FOR_JAR:
				if (command.getID().equals(COMMAND_IN_UPDATE_DATA)) {
					addCommandToBuffer(command);
				}
				break;
			case UPDATING_STAGE_TWO:
				if (command.getID().equals(COMMAND_IN_CHECK_UPDATE_SERVER_CONFIRM)) {
					addCommandToBuffer(command);
				}
				break;
			case UPDATING_STAGE_THREE:
				if (command.getID().equals(COMMAND_IN_CHECK_UPDATE_SERVER_CONFIRM)) {
					addCommandToBuffer(command);
				}
				break;
			case UPDATING_DONT_DISTURB:
				// dont get any message
				break;
			default:
				Logger.log("We got a command (" + command.getID() + ") in an unknown state (" + updateState + ").", Logger.CMP_SERVICE, Logger.LEVEL_WARNING);
				break;
			}
		}
	}

	private void addCommandToBuffer(Command command) {
		try {
			commandBuffer.put(command);
		} catch (InterruptedException e) {
			Logger.log("Could not add command (" + command.getID() + ") to commandBuffer.", Logger.CMP_SERVICE, Logger.LEVEL_WARNING);
		}
	}

	Command getCommandFromBuffer(String cmdID) {
		Command cmd;
		while(true) {
			try {
				cmd = commandBuffer.take();
				if (cmd.getID().equals(cmdID)) {
					return cmd;
				}
			} catch (InterruptedException e) {
				Logger.log("Could not take command (" + cmdID + ") from commandBuffer.", Logger.CMP_SERVICE, Logger.LEVEL_WARNING);
			}
		}
	}
	
	Command getCommandFromBuffer(String cmdID, int maxWaitInSeconds) {
		Command cmd = null;
		long stopTimeMillis = System.currentTimeMillis() + maxWaitInSeconds*1000;
		while(true) {
			try {
				long currentTimeMillis = System.currentTimeMillis();
				if (stopTimeMillis < currentTimeMillis) {
					return cmd;
				}
				cmd = commandBuffer.poll(stopTimeMillis - currentTimeMillis, TimeUnit.MILLISECONDS);
				if (cmd == null) {
					return cmd;
				}
				if (cmd.getID().equals(cmdID)) {
					return cmd;
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	boolean saveJar(byte[] content, String jarPath) {
		File jarFile = new File(jarPath);
		if (jarFile.exists()) {
			jarFile.delete();
		}
		try {
			jarFile.createNewFile();
			FileOutputStream os = new FileOutputStream(jarFile);
			os.write(content);
			os.flush();
			os.close();			
		} catch (IOException e) {
			log("Could not save jar: " + jarPath, Logger.CMP_SERVICE, Logger.LEVEL_ERROR);
			return false;
		}
		return true;
	}

	// +----------------------------------------------------------------------------------------------+
	// |                          MonitoredDeviceStart Thread                                         |
	// +----------------------------------------------------------------------------------------------+
	/**
	 * Runs on devices when they are started. Right now just handshakes with monitor to see if socket
	 * communication is working. In later implementations they could also try to talk with the update 
	 * server to see that tunnels work.
	 */
	private class MonitoredDeviceStartThread extends Thread {
		@Override
		public void run(){
			log("MonitoredDeviceStart Thread started. Performing startup check...", Logger.CMP_SERVICE, Logger.LEVEL_DEBUG);
			updateState = UpdateState.STARTUP;
			while (true) {
				String msg = socketListener.getMsg();
				if (msg.equals(UPDATE_PROTOCOL_CHECK_SOCKET)) {
					socketSender.sendMsg(UPDATE_PROTOCOL_CHECK_SOCKET_CONFIRM, SocketSender.TRY_FOREVER);
					setAsFullyOperational();
				} else if (msg.equals(UPDATE_PROTOCOL_CHECK_UPDATE_SERVER)) {
					// Get UpdateServer's deviceID in order to connect to it
					String updateServerDeviceID = socketListener.getMsg();
					Writable writableConnToUpdateServer = getWritableConnectionToService(new DeviceID(updateServerDeviceID), UpdateDistributionService.SERVICE_NAME, -1);

					// request response from update server in order to test Palcom tunnel/communication
					Command confirmRequestCmd = getProtocolHandler().findCommand(COMMAND_OUT_CHECK_UPDATE_SERVER);
					log("Sending confirmation request to update server", Logger.CMP_SERVICE, Logger.LEVEL_DEBUG);
					sendTo(writableConnToUpdateServer, confirmRequestCmd);
					
					// wait for response from server
					log("Waiting for confirmation from update server.", Logger.CMP_SERVICE, Logger.LEVEL_DEBUG);
					getCommandFromBuffer(COMMAND_IN_CHECK_UPDATE_SERVER_CONFIRM);
					
					log("Got response from update server!", Logger.CMP_SERVICE, Logger.LEVEL_DEBUG);
					socketSender.sendMsg(UPDATE_PROTOCOL_CHECK_UPDATE_SERVER_CONFIRM, SocketSender.TRY_FOREVER);
				} else if (msg.equals(UPDATE_PROTOCOL_FINISH_DEVICE_STARTUP_CHECK)) {
					socketListener.closeSocket();
					// Send ACK when socketListeners socket is closed. Otherwise it will block the
					// port for next monitored device.
					socketSender.sendMsg(UPDATE_PROTOCOL_FINISH_DEVICE_STARTUP_CHECK_ACK, SocketSender.TRY_FOREVER);
					log("MonitoredDeviceStart Thread startup check finished.", Logger.CMP_SERVICE, Logger.LEVEL_INFO);
					updateState = UpdateState.NONE;
					break;
				} else if (msg.equals(UPDATE_PROTOCOL_STAGE_TWO)) {
					updateState = UpdateState.UPDATING_STAGE_TWO;
					new UpdateStageTwoThread(getUpdaterService(), socketListener, socketSender).start();
					break;
				}
			}
			log("MonitoredDeviceStart Thread done. Thread killed.", Logger.CMP_SERVICE, Logger.LEVEL_DEBUG);
		}
	}
	
	// +----------------------------------------------------------------------------------------------+
	// |                          PalComStarterStart Thread                                           |
	// +----------------------------------------------------------------------------------------------+
	/**
	 * Runs on PalcomStarter when it is started. Right now just handshakes with monitor to see if socket
	 * communication is working. In later implementations it could also try to talk with the update 
	 * server to see that tunnels work.
	 */
	private class PalComStarterStartThread extends Thread {
		@Override
		public void run() {
			log("PalComStarter Start Thread started.", Logger.CMP_SERVICE, Logger.LEVEL_DEBUG);
			setAsFullyOperational();
			if (continueUpdateStageThree) {
				// We are in the middle of an update. Let's carry on with it!
				updateState = UpdateState.UPDATING_STAGE_THREE;
				UpdateStageThreeThread updateStageThreeThread = new UpdateStageThreeThread(getUpdaterService(), socketListener, socketSender);
				updateStageThreeThread.start();
				while (true) {
					try {
						updateStageThreeThread.join();
						break;
					} catch (InterruptedException e) {
						log("Got interrupted while waiting for UpdateStageThree Thread. Things will go BAD if we continue so I will try to join again...", Logger.CMP_SERVICE, Logger.LEVEL_WARNING);
					}
				}
			}
			updateState = UpdateState.NONE;
			updateServerConnectionListener.addUpdateServerListener();
			// Startup update check
			String tmp = monitoringProperties.getProperty(NAMESPACE_UPDATERSERVICE_GENERAL, KEY_UPDATE_ABORTED);
			if (tmp != null) { // then update was aborted last time so we need to remember it
				updateAborted = Integer.valueOf(tmp);
				log("Update was just aborted (total " + updateAborted + " times)", Logger.CMP_SERVICE, Logger.LEVEL_INFO);
				if (updateAborted > UPDATE_ABORTED_MANY_TIMES) {
					updateAbortedDelay = System.currentTimeMillis() + UPDATE_ABORTED_MANY_TIMES_DELAY_SECONDS*1000;
				} else {
					updateAbortedDelay = System.currentTimeMillis() + UPDATE_ABORTED_DELAY_SECONDS*1000;					
				}
				log("Will wait " + (updateAbortedDelay/1000) + " seconds before trying to update again.", Logger.CMP_SERVICE, Logger.LEVEL_INFO);
				Timer t = new Timer();
				t.schedule(new TimerTask() {
					@Override
					public void run() {
						updateServerConnectionListener.checkLatestVersion();
					}
				}, UPDATE_ABORTED_DELAY_SECONDS*1000);
			} else {
				updateServerConnectionListener.checkLatestVersion();
			}
			monitor.start();
			log("PalComStarter startup check complete.", Logger.CMP_SERVICE, Logger.LEVEL_INFO);
			log("PalComStarter Start Thread done. Thread killed.", Logger.CMP_SERVICE, Logger.LEVEL_DEBUG);
		}
	}
		
	/**
	 * @param deviceID
	 * @param serviceName
	 * @param maxSecondsToWait if -1 it will try forever
	 * @return
	 */
	Writable getWritableConnectionToService(DeviceID deviceID, String serviceName, int maxSecondsToWait) {
		long timeBetweenReadyChecks = 100; // milliseconds
		Writable writableConn = null;
		PalcomDevice pd = container.getDiscoveryManager().getDevice(deviceID);
		long timeToStop = System.currentTimeMillis() + maxSecondsToWait*1000;
		while (!pd.isReady()) {
			if (maxSecondsToWait != -1 && System.currentTimeMillis() > timeToStop) {
				return null;
			}
			try {
				Thread.sleep(timeBetweenReadyChecks);
			} catch (InterruptedException e) {
				return null;
			}
		}
		log("Device with ID " + deviceID + " is ready.", Logger.CMP_SERVICE, Logger.LEVEL_DEBUG);
		PalcomServiceList psl = pd.getServiceList();
		PalcomService ps = null;
		try {
			for (int i = 0; i < psl.getNumService(); ++i) {
				ps = (PalcomService) psl.getService(i);
				if(ps.getName().equals(serviceName)) {
					log("Service with name " + serviceName + " found.", Logger.CMP_SERVICE, Logger.LEVEL_DEBUG);
					break;
				} else {
					ps = null;
				}
			}
		} catch (ResourceException e) {/* handled below */}
		if (ps == null) {
			log("Did not find service with name " + serviceName + " on device with ID " + deviceID + ".", Logger.CMP_SERVICE, Logger.LEVEL_DEBUG);
			return null;
		}
		while (!ps.isReady()) {
			if (maxSecondsToWait != -1 && System.currentTimeMillis() > timeToStop) {
				return null;
			}
			try {
				Thread.sleep(5*1000);
			} catch (InterruptedException e) {
				return null;
			}
		}
		try {
			Connection conn = ps.connectTo(getConnectionHandler(), getLocalAddress(), 5000, PalComVersion.DEFAULT_SERVICE_INTERACTION_PROTOCOL);
			try {
				conn.open();
				log("Successfully connected to " + serviceName + " on " + deviceID, Logger.CMP_SERVICE, Logger.LEVEL_DEBUG);
				if (conn instanceof Writable) {
					writableConn = (Writable) conn;
				}
			} catch (IllegalStateException e) {
				log("Connection to " + serviceName + " is already open.", Logger.CMP_SERVICE, Logger.LEVEL_DEBUG);
			} catch (NoSuchDeviceException e) {
				log("Device " + deviceID + "cannot be found.", Logger.CMP_SERVICE, Logger.LEVEL_DEBUG);
			} catch (IOException e) {
				// Auto-generated catch block
				e.printStackTrace();
			}
		} catch (TreeUpdateException e) {
			// Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// Auto-generated catch block
			e.printStackTrace();
		} catch (ResourceException e) {
			// Auto-generated catch block
			e.printStackTrace();
		}
		return writableConn;
	}
	
	void log(String msg, int component, int logLevel) {
		// Uncommment this to use standard logging
//		Logger.log(msg, component, logLevel);
		
		// Uncomment this to see color coded log messages.
//		boolean linuxColorCoding = true;
//		saneLog(msg, component, logLevel, linuxColorCoding, false);
		
		// Uncomment this and set log level to Logger.NONE in order to see what is happening when debugging.
		boolean linuxColorCoding = true;
		saneLog(msg, component, logLevel, linuxColorCoding, true);
	}
	
	private void saneLog(String msg, int component, int logLevel, boolean linuxColorCoding, boolean stripped) {
		String red = "";
		String green = "";
		String resetColor = "";

		// Using color coding for linux terminal
		if (linuxColorCoding) {
			red = "\033[31m";
			green = "\033[32m";
			resetColor = "\033[0m";
		}
		
		String extraInfo;
		DeviceProperties dp;
		try {
			dp = new DeviceProperties(new DeviceID("monitoring"), HostFileSystems.getGlobalRoot(), null, "Monitoring properties. Generated " + new Date());
			if (isMonitor) {
				extraInfo = red + container.getName() + "(" + dp.getProperty(NAMESPACE_UPDATERSERVICE_DEVICE_TYPE_VERSION, PALCOMSTARTER_DEVICE_TYPE) + ")";
			} else {
				extraInfo = green + container.getName() + "(" + dp.getProperty(NAMESPACE_UPDATERSERVICE_DEVICE_TYPE_VERSION, "TheThing") + ")";
			}
		} catch (IOException e) {
			if (isMonitor) {
				extraInfo = red + container.getName();
			} else {
				extraInfo = green + container.getName();
			}
		}
		String print = extraInfo + ": " + resetColor + msg;
		if (stripped) {
			System.err.println(print);			
		} else {
			Logger.log(print, component, logLevel);
		}
	}

	synchronized void setUpdateState(UpdateState state) {
		updateState = state;
	}
	boolean sendPalComMessage(Writable conn, Command cmd) {
		int status = super.sendTo(conn, cmd);
		switch (status) {
		case SEND_BUFFER_FULL:
			log("Send buffer full.", Logger.CMP_SERVICE, Logger.LEVEL_WARNING);
			return false;
		case SEND_ERROR:
			log("Send error.", Logger.CMP_SERVICE, Logger.LEVEL_ERROR);
			return false;
		case SEND_OK:
			log("Successfully sent command " + cmd.getID(), Logger.CMP_SERVICE, Logger.LEVEL_DEBUG);
			return true;
		default:
			break;
		}
		return false;
	}

	Command getCommand(String cmdID) {
		return getProtocolHandler().findCommand(cmdID);
	}
	
	UpdaterService getUpdaterService() {
		return this;
	}
}
