package se.lth.cs.palcom.updaterservice;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import ist.palcom.resource.descriptor.Command;
import ist.palcom.resource.descriptor.DeviceID;
import ist.palcom.resource.descriptor.Group;
import ist.palcom.resource.descriptor.PRDService;
import ist.palcom.resource.descriptor.Param;
import ist.palcom.resource.descriptor.ServiceID;
import se.lth.cs.palcom.common.NoSuchDeviceException;
import se.lth.cs.palcom.common.PalComVersion;
import se.lth.cs.palcom.common.TreeUpdateException;
import se.lth.cs.palcom.communication.connection.Connection;
import se.lth.cs.palcom.communication.connection.Readable;
import se.lth.cs.palcom.communication.connection.Writable;
import se.lth.cs.palcom.device.AbstractDevice;
import se.lth.cs.palcom.device.DeviceProperties;
import se.lth.cs.palcom.discovery.Resource;
import se.lth.cs.palcom.discovery.ResourceException;
import se.lth.cs.palcom.discovery.ResourceListener;
import se.lth.cs.palcom.discovery.proxy.PalcomDevice;
import se.lth.cs.palcom.discovery.proxy.PalcomService;
import se.lth.cs.palcom.discovery.proxy.PalcomServiceList;
import se.lth.cs.palcom.discovery.proxy.implementors.DeviceProxy;
import se.lth.cs.palcom.filesystem.HostFileSystems;
import se.lth.cs.palcom.palcomstarter.PalComStarter;
import se.lth.cs.palcom.service.AbstractSimpleService;
import se.lth.cs.palcom.service.ServiceTools;
import se.lth.cs.palcom.service.command.CommandServiceProtocol;
import se.lth.cs.palcom.service.distribution.UnicastDistribution;
import se.lth.cs.palcom.updatedistributionservice.UpdateDistributionService;
import se.lth.cs.palcom.util.configuration.DeviceList;

/** 
 * Service that can either act by monitoring or being monitored. When monitoring, can also update all its monitored 
 * devices by communicating with and getting updates from an {@link UpdateDistributionService}. 
 * @author Christian Hernvall
 *
 */
public class UpdaterService extends AbstractSimpleService {

	public static final DeviceID CREATOR = new DeviceID("X:mojo");
	public static final ServiceID SERVICE_VERSION = new ServiceID(CREATOR, "UP1.0.0", CREATOR, "UP1.0.0");
	public static final String SERVICE_NAME = "UpdaterService";

	public static final String COMMAND_IN_UPDATE_DEVICE_TYPES = "update single device type";
	private static final String COMMAND_IN_STOP_MONITORED_DEVICES = "stop all monitored devices";
	public static final String COMMAND_IN_UPDATE_DATA = "updateData";
	public static final String COMMAND_IN_CHECK_UPDATE_SERVER_CONFIRM = "I hear you!";
	private static final String COMMAND_IN_ABORT_UPDATE = "abort update!";
	private static final String COMMAND_IN_KILL = "kill";
	private static final String COMMAND_IN_INITIATE_STAGE_TWO = "initiate updating stage two";

	private static final String COMMAND_IN_DISABLE_MONITORING = "disable monitor";
	private static final String COMMAND_IN_ENABLE_MONITORING = "enable monitor";
	private static final String COMMAND_IN_LIST_MONITORED_DEVICES = "list all monitored devices";
	private static final String COMMAND_IN_KILL_DEVICE_BY_INDEX = "kill device by index";
	private static final String COMMAND_IN_START_DEVICE_BY_INDEX = "start device by index";
	private static final String COMMAND_IN_RESTART_DEVICE_BY_INDEX = "restart device by index";
	private static final String COMMAND_IN_RESET_UPDATE_ABORTED_COUNTER = "reset update aborted counter";

	public static final String COMMAND_OUT_UPDATE_CONTENT_REQUEST = "gief the jar!";
	public static final String COMMAND_OUT_CHECK_UPDATE_SERVER = "do you hear me?";
	private static final String COMMAND_OUT_KILL = COMMAND_IN_KILL;
	public static final String COMMAND_OUT_CHECK_LATEST_VERSION = "latest version?";
	private static final String COMMAND_OUT_LIST_MONITORED_DEVICES = "list of all monitored devices";
	public static final String COMMAND_OUT_BENCHMARK_END = "benchmark end";
	private static final String COMMAND_OUT_INITIATE_STAGE_TWO = COMMAND_IN_INITIATE_STAGE_TWO;

	public static final String PARAM_VALUE_SEPARATOR = ",,,";
	public static final String PARAM_NO_ENTRY = "no entry";
	public static final String PARAM_VERSION = "version";
	public static final String PARAM_DEVICE_TYPE = "device type";
	public static final String PARAM_UPDATE_CONTENT = "jar content";
	public static final String PARAM_SERVICEINSTANCEID = "serviceInstanceID";
	private static final String PARAM_MONITORED_DEVICES = "monitored devices";
	private static final String PARAM_MONITORED_DEVICE_INDEX = "monitored device index";

	private static final String NAMESPACE_UPDATERSERVICE_MONITORED_DEVICE_NAMES = "monitoredDeviceNames";
	private static final String NAMESPACE_MONITORED_DEVICE = "monitoredDevice-";
	private static final String NAMESPACE_UPDATERSERVICE_DEVICE_TYPE_VERSION = "deviceTypeVersion";
	private static final String NAMESPACE_UPDATERSERVICE_GENERAL = "general";
//	private static final String UPDATERSERVICE_NAMESPACE = "UpdaterService";

	private static final String KEY_MONITORED_DEVICE_ID = "ID";
	private static final String KEY_MONITORED_DEVICE_TYPE = "type";
	private static final String KEY_UPDATE_SERVER_DEVICE_ID = "updateServerDeviceID";
	private static final String KEY_UPDATE_ABORTED = "updateAborted";

	private static final String PROPERTY_MONITORED_DEVICE_ENABLED = "enabled";

	private static final String UPDATE_PROTOCOL_KILL = "kill";
	private static final String UPDATE_PROTOCOL_KILL_ACK = "kill ack";
	private static final String UPDATE_PROTOCOL_ABORT = "abort!";
	private static final String UPDATE_PROTOCOL_CHECK_SOCKET = "socket working?";
	private static final String UPDATE_PROTOCOL_CHECK_SOCKET_CONFIRM = "socket working!";
	private static final String UPDATE_PROTOCOL_CHECK_UPDATE_SERVER = "update server hear you?";
	private static final String UPDATE_PROTOCOL_CHECK_UPDATE_SERVER_CONFIRM = "update server hear me!";
	private static final String UPDATE_PROTOCOL_FINISH_DEVICE_STARTUP_CHECK = "finish device startup check";
	private static final String UPDATE_PROTOCOL_FINISH_DEVICE_STARTUP_CHECK_ACK = "finish device startup check ACK";
	private static final String UPDATE_PROTOCOL_STAGE_TWO = "update stage two";
	private static final String UPDATE_PROTOCOL_FINISH_STAGE_TWO = "finish stage two";
	private static final String UPDATE_PROTOCOL_FINISH_STAGE_TWO_ACK = "finish stage two ACK";

	static final String MSG_SHUT_DOWN_THREAD = "shut down thread";

	private static final String PALCOMSTARTER_DEVICE_TYPE = "PalComStarter";

	private static final int UPDATE_ABORTED_DELAY_SECONDS = 30;
	private static final int MAX_TIMES_TO_RETRY_UPDATE = 3;
	private static final int PALCOMSTARTER_SOCKET_PORT = 13370;
	private static final int MONITORED_DEVICE_SOCKET_PORT = 13371;

	private enum UpdateState {
		NONE, UPDATING_INITIAL, UPDATING_WAITING_FOR_JAR, UPDATING_KILLING_CURRENT, UPDATING_STARTING_NEW, UPDATING_FALLBACK_TIMER_CHECK_SOCKET, UPDATING_STAGE_TWO, STARTUP, UPDATING_FALLBACK_TIMER_CHECK_UPDATE_SERVER, UPDATING_SENDING_JAR, UPDATING_DONT_DISTURB, UPDATING_STAGE_THREE,
	}
	private static final int MAJOR = 0;
	private static final int MINOR = 1;
	private static final int PATCH = 2;
	private static final int IDENTICAL_VERSION = 3;
	private static final int DOWNGRADE = 4;
	private static final int INCOMPATIBLE_VERSION_SCHEME = 5;
	
	private UpdateState updateState = UpdateState.NONE;

	private MonitoringThread monitor;
	private LinkedBlockingQueue<Command> commandBuffer;
	private SocketListenerThread socketListener;
	private SocketSender socketSender;
	private UpdateServerConnectionListener updateServerConnectionListener;
	private boolean isMonitor = false;
	private String updateServerDeviceID;
	private DeviceProperties monitoringProperties;
	private Integer updateAborted = 0;
	private long updateAbortedDelay;
	private boolean continueUpdateStageThree = false;

	public UpdaterService(AbstractDevice container) {
		this(container, ServiceTools.getNextInstance(SERVICE_VERSION));
	}
	
	public UpdaterService(AbstractDevice container, boolean continueUpdateStageThree) {
		super(container, SERVICE_VERSION, "P1", "v0.0.1", "UpdaterService",
				ServiceTools.getNextInstance(SERVICE_VERSION), "Updates PalCom devices",
				new UnicastDistribution(true));
		this.continueUpdateStageThree = continueUpdateStageThree;
		printderp("Continue updating stage three: " + continueUpdateStageThree);
		configureService();
	}
	
	public UpdaterService(AbstractDevice container, String instance) {
		super(container, SERVICE_VERSION, "P1", "v0.0.1", "UpdaterService",
				instance, "Updates PalCom devices",
				new UnicastDistribution(true));
		configureService();
	}
	
	private void configureService() {
//		HostFileSystems.getDeviceRoot(deviceId)
//		DeviceList.getConfFolder(deviceType)
		// Used as override during testing to use a TheThing as PalComStarter
		// TODO Should not be needed in the future
		try {
			if(container.getDeviceRootFileSystem().getFile("IamMonitor", false).exists()) {
				isMonitor = true;
			} else {
				isMonitor = false;
			}
		} catch (IOException e) {
			isMonitor = false;
		}
		if (isMonitor || container instanceof PalComStarter) {
			isMonitor = true;
			try {
				monitoringProperties = new DeviceProperties(new DeviceID("monitoring"), HostFileSystems.getGlobalRoot(), null, "Monitoring properties. Generated " + new Date());
				String[] monitoredDeviceNames = monitoringProperties.getKeys(NAMESPACE_UPDATERSERVICE_MONITORED_DEVICE_NAMES);
				monitor = new MonitoringThread();
				for (String deviceName: monitoredDeviceNames) {
					if (!monitoringProperties.getProperty(NAMESPACE_UPDATERSERVICE_MONITORED_DEVICE_NAMES, deviceName).equals(PROPERTY_MONITORED_DEVICE_ENABLED)){
						// Do not load the device info if it is not set as enabled
						continue;
					}
					String deviceSpecificNamespace = NAMESPACE_MONITORED_DEVICE + deviceName;
					String monitoredDeviceID = monitoringProperties.getProperty(deviceSpecificNamespace, KEY_MONITORED_DEVICE_ID);
					String monitoredDeviceType = monitoringProperties.getProperty(deviceSpecificNamespace, KEY_MONITORED_DEVICE_TYPE);
					if (monitoredDeviceType == null) {
						printderp("ERROR: Device type is not specified in the configuration: " + deviceSpecificNamespace + "@" + KEY_MONITORED_DEVICE_TYPE);
						printderp("Will not monitor device with name: " + deviceName);
						continue;
					}
					String monitoredDeviceVersion = monitoringProperties.getProperty(NAMESPACE_UPDATERSERVICE_DEVICE_TYPE_VERSION, monitoredDeviceType);
					if (monitoredDeviceVersion == null) {
						printderp("ERROR: Device version is not specified in the configuration: " + deviceSpecificNamespace + "@" + KEY_MONITORED_DEVICE_TYPE);
						printderp("Will not monitor device with name: " + deviceName);
						continue;
					}
					if (monitoredDeviceID == null) {
						//TODO Generate a new device if it is missing its device ID
						printderp("ERROR: Device ID is not specified in the configuration: " + deviceSpecificNamespace + "@" + KEY_MONITORED_DEVICE_TYPE);
						printderp("Will not monitor device with name: " + deviceName);
						continue;
					}
					monitor.addNewMonitoredDevice(monitoredDeviceID, deviceName, monitoredDeviceType, monitoredDeviceVersion);
				}
				updateServerDeviceID = monitoringProperties.getProperty(NAMESPACE_UPDATERSERVICE_GENERAL, KEY_UPDATE_SERVER_DEVICE_ID);
				if (updateServerDeviceID == null) {
					printderp("UpdateServer device ID is not set in configuration: " + NAMESPACE_UPDATERSERVICE_GENERAL + "@" + KEY_UPDATE_SERVER_DEVICE_ID);;
					printderp("Will not be able to communicate with or receive updates from Update Server.");
				}
				socketListener = new SocketListenerThread(this, PALCOMSTARTER_SOCKET_PORT);
				socketSender = new SocketSender(this, MONITORED_DEVICE_SOCKET_PORT);
				updateServerConnectionListener = new UpdateServerConnectionListener();
			} catch (IOException e) {
				e.printStackTrace();
			}
		} 
		if (!isMonitor) { // This means that this service is running on a monitored device, not a PalComStarter. Reverse the ports.
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
//		socketSender.start();
		if (isMonitor) {
			printderp("I am a monitor");
			new PalComStarterStartThread().start();
		} else {
			printderp("I HAVE NO CHILDREN TO MONITOR! GAHHAH! I AM NO MONITOR");
			new MonitoredDeviceStartThread().start();
			// wait to set as fully operational until socket is tested (done in DeviceStart Thread)
		}
	}
	
	private void stopDevice() {
		printderp("Stopping device.");
		stopUpdaterService();
//		container.stop(); // Throwing errors. Better way to stop the device?
		System.exit(0); // needed? container.stop() doesn't kill theThing completely so this will do for now.
	}
	
	private void stopUpdaterService() {
		stop();
	}
	
	public void stopHelperThreads() {
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
						// We are now in the updating state. Commands that could interrupt the procedure are blocked (ignored)
						updateState = UpdateState.UPDATING_INITIAL;
						printderp("Got new update from UpdateServer");
						String deviceTypes = toUTF8String(command.findParam(PARAM_DEVICE_TYPE).getData());
						String newVersions = toUTF8String(command.findParam(PARAM_VERSION).getData());
						if (deviceTypes == null) {
							printderp("Device types is null. Will not update.");
						} else if (newVersions == null) {
							printderp("Versions is null. Will not update.");
						} else {
							String[] splitDeviceTypes = deviceTypes.split(PARAM_VALUE_SEPARATOR);
							String[] splitNewVersions = newVersions.split(PARAM_VALUE_SEPARATOR);
							if (splitDeviceTypes.length < 1) {
								printderp("There are no device types. Will not update.");
							} else if (splitNewVersions.length < 1) {
								printderp("There are no versions. Will not update.");
							} else {
								new UpdateStageOneThread((Writable) conn, splitDeviceTypes, splitNewVersions).start();														
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
						printderp("Update Aborted counter reset.");
					} else {
						printderp("Received unknown command: " + command.getID());
					}
				} else { // is not monitor
					if (command.getID().equals(COMMAND_IN_INITIATE_STAGE_TWO)) {
						updateState = UpdateState.UPDATING_STAGE_TWO;
						socketListener.reopenSocket();
						new UpdateStageTwoThread().start();
					} else {
						printderp("Received unknown command: " + command.getID());
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
				break;
			}
		}
	}

	private void addCommandToBuffer(Command command) {
		try {
			commandBuffer.put(command);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public Command getCommandFromBuffer(String cmdID) {
		Command cmd;
		while(true) {
			try {
				cmd = commandBuffer.take();
				if (cmd.getID().equals(cmdID)) {
					return cmd;
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	public Command getCommandFromBuffer(String cmdID, int maxWaitInSeconds) {
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

	private boolean copyCurrentFilesystem(String currentBaseURL, String newBaseURL) {
		try {
			File currentFS = new File(currentBaseURL + "/PalcomFilesystem");
			File newFS = new File(newBaseURL + "/PalcomFilesystem");
			RecursiveDirHandler recursiveDirHandler = new RecursiveDirHandler(currentFS.toPath(), newFS.toPath());
			if(newFS.exists()) {
				printderp("New PalCom filesystem already exists. Deleting it first: " + newFS);
				recursiveDirHandler.changeAction(RecursiveDirHandler.DELETE);
				Files.walkFileTree(newFS.toPath(), recursiveDirHandler);
			}
			newFS.mkdirs();
			newFS.delete();
			if (currentFS.exists() && currentFS.isDirectory()) {
				recursiveDirHandler.changeAction(RecursiveDirHandler.COPY);
				printderp("Copying device configuration (PalCom filesystem) from/to:\n" + currentFS + " /\n" + newFS);
				Files.walkFileTree(currentFS.toPath(), recursiveDirHandler);
			} else {
				printderp("Current filesystem not found at:" + currentFS);
			}
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}
	
	private boolean deleteFilesystem(String baseURL, boolean deleteOnJVMExit) {
		File FS = new File(baseURL + "/PalcomFilesystem");
		RecursiveDirHandler recursiveDirHandler;
		if (deleteOnJVMExit) {
			recursiveDirHandler = new RecursiveDirHandler(RecursiveDirHandler.DELETE_ON_EXIT);			
		} else {
			recursiveDirHandler = new RecursiveDirHandler(RecursiveDirHandler.DELETE);
		}
		try {
			Files.walkFileTree(FS.toPath(), recursiveDirHandler);
		} catch (IOException e) {
			return false;
		}
		return true;
	}

	private boolean saveJar(byte[] content, String jarPath) {
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
			printderp("Could not save jar: " + jarPath);
			return false;
		}
		return true;
		// File is not DEX'd. Is it necessary?
	}

	public void printderp(String msg) {
		String print;
		DeviceProperties dp;
		try {
			dp = new DeviceProperties(new DeviceID("monitoring"), HostFileSystems.getGlobalRoot(), null, "Monitoring properties. Generated " + new Date());
			if (isMonitor) {
				print = "\033[31m" + container.getName() + "(" + dp.getProperty(NAMESPACE_UPDATERSERVICE_DEVICE_TYPE_VERSION, PALCOMSTARTER_DEVICE_TYPE) + ")";
			} else {
				print = "\033[32m" + container.getName() + "(" + dp.getProperty(NAMESPACE_UPDATERSERVICE_DEVICE_TYPE_VERSION, "TheThing") + ")";
			}
		} catch (IOException e) {
			if (isMonitor) {
					print = "\033[31m" + container.getName();
				} else {
					print = "\033[32m" + container.getName();
				}
		}
		
		System.err.println(print + ": \033[0m" + msg);
	}
	


	// +----------------------------------------------------------------------------------------------+
	// |                          MonitoredDeviceStart Thread                                                  |
	// +----------------------------------------------------------------------------------------------+
	/**
	 * Runs on devices when they are started. Right now just handshakes with monitor to see if socket
	 * communication is working. In later implementations they could also try to talk with the update 
	 * server to see that tunnels work.
	 */
	private class MonitoredDeviceStartThread extends Thread {
		@Override
		public void run(){
			printderp("MonitoredDeviceStart Thread started");
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
					printderp("Sending confirmation request to update server");
					sendTo(writableConnToUpdateServer, confirmRequestCmd);
					
					// wait for response from server
					printderp("Waiting for confirmation from update server.");
					getCommandFromBuffer(COMMAND_IN_CHECK_UPDATE_SERVER_CONFIRM);
					
					printderp("Got response from update server!");
					socketSender.sendMsg(UPDATE_PROTOCOL_CHECK_UPDATE_SERVER_CONFIRM, SocketSender.TRY_FOREVER);
				} else if (msg.equals(UPDATE_PROTOCOL_FINISH_DEVICE_STARTUP_CHECK)) {
					socketListener.closeSocket();
					// Send ACK when socketListeners socket is closed. Otherwise it will block the
					// port for next monitored device.
					socketSender.sendMsg(UPDATE_PROTOCOL_FINISH_DEVICE_STARTUP_CHECK_ACK, SocketSender.TRY_FOREVER);
					printderp("DeviceStartThread startup check finished.");
					updateState = UpdateState.NONE;
					break;
				} else if (msg.equals(UPDATE_PROTOCOL_STAGE_TWO)) {
					updateState = UpdateState.UPDATING_STAGE_TWO;
					new UpdateStageTwoThread().start();
					break;
				}
			}
			printderp("MonitoredDeviceStart Thread done. Thread killed.");
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
			printderp("PalComStarter Start Thread started.");
			setAsFullyOperational();
			if (continueUpdateStageThree) {
				// We are in the middle of an update. Let's carry on with it!
				updateState = UpdateState.UPDATING_STAGE_THREE;
				UpdateStageThreeThread updateStageThreeThread = new UpdateStageThreeThread();
				updateStageThreeThread.start();
				while (true) {
					try {
						updateStageThreeThread.join();
						break;
					} catch (InterruptedException e) {
						printderp("Got interrupted while waiting for UpdateStageThree Thread. Things will go BAD if we continue so I will try to join again...");
					}
				}
			}
			updateState = UpdateState.NONE;
			updateServerConnectionListener.addUpdateServerListener();
			// startup update check
			String tmp = monitoringProperties.getProperty(NAMESPACE_UPDATERSERVICE_GENERAL, KEY_UPDATE_ABORTED);
			if (tmp != null) { // then update was aborted last time so we need to remember it
				updateAborted = Integer.valueOf(tmp);
				printderp("Update was just aborted (total " + updateAborted + " times)");
				printderp("Will wait " + UPDATE_ABORTED_DELAY_SECONDS + " seconds before trying to update again.");
				updateAbortedDelay = System.currentTimeMillis() + UPDATE_ABORTED_DELAY_SECONDS*1000;
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
			
			printderp("PalComStarter Start Thread done. Thread killed.");
		}
	}
	
	private class UpdateDescription {
		String deviceType;
		String version;
		int updateType;
		String pathToExec;
		public UpdateDescription(String deviceType, String deviceTypeVersion, int updateType, String pathToExec) {
			this.deviceType = deviceType;
			this.version = deviceTypeVersion;
			this.updateType = updateType;
			this.pathToExec = pathToExec;
		}
	}
	
	// +----------------------------------------------------------------------------------------------+
	// |                          UpdateStageOne Thread                                               |
	// +----------------------------------------------------------------------------------------------+
	/**
	 * Thread started when the monitored devices should be updated. Goes through the updating process
	 * and changes updatestates.
	 */
	
	private class UpdateStageOneThread extends Thread {
		private static final int MAX_SECONDS_WAIT_FOR_DATA = 15;
		private static final int MAX_SECONDS_WAIT_FOR_DEVICE = 10;
		private String pathToFS;
		private Writable conn;
		private String[] deviceTypes;
		private String[] newVersions;
		private LinkedList<UpdateDescription> monitoredDeviceTypesToUpdate;
		private UpdateDescription palComStarterUpdateDescription;
		private boolean performMonitoredDeviceUpdate;
		private boolean performPalComStarterUpdate;
		private boolean performMajorUpdate;

		public UpdateStageOneThread(Writable conn, String[] deviceTypes, String[] deviceTypeNewVersions) {
			this.conn = conn;
			this.deviceTypes = deviceTypes;
			this.newVersions = deviceTypeNewVersions;
			monitoredDeviceTypesToUpdate = new LinkedList<UpdateDescription>();
		}

		@Override
		public void run() {
			printderp("UpdateStageOne Thread started.");		
			// Preparing and setting some variables needed later in the process and in case of emergency abort
			if (!stageOnePreparations()) {
				printderp("No update will be performed.");
				updateState = UpdateState.NONE;
				return;
			}
			
			try {
				stageOne();
			} catch (Throwable t) {
				// If stage one could not be completed due to any reason, such as outOfMemoryError, we try to abort gracefully
				abortUpdateStageOne("Update Stage One received unknown error/exception. Trying to abort gracefully. Message:\n");
				t.printStackTrace();
			}
		}
		
		private boolean stageOnePreparations() {
			// Check if we recently tried to update and failed
			if (updateAborted > 0 && updateAbortedDelay > System.currentTimeMillis()) {
				printderp("Recently tried to update (" + updateAborted +" times) and failed. Updating disabled for " + (updateAbortedDelay - System.currentTimeMillis())/1000 + "s.");
				return false;
			}
			if (updateAborted >= MAX_TIMES_TO_RETRY_UPDATE) {
				printderp("Tried to update " + MAX_TIMES_TO_RETRY_UPDATE + " times before. Will not try again.");
				// TODO: Let the UpdateServer know that we failed many times and won't try again?
				return false;
			}
			try {
				pathToFS = HostFileSystems.getGlobalRoot().getURL().replace("/PalcomFilesystem/global", "").replace("file:", "");
			} catch (IOException e1) {
				printderp("Could not get path to PalcomFilesystem. Can not perform update.");
				return false;
			}
			// Check what type of updates we are to perform
			performMonitoredDeviceUpdate = false;
			performPalComStarterUpdate = false;
			performMajorUpdate = false; // TODO check for this when the other stuff is working
			for (int i = 0; i < deviceTypes.length; ++i) {
				String deviceType = deviceTypes[i];
				String newVersion = newVersions[i];
				if (monitor.monitorsDeviceType(deviceType) || deviceType.equals(PALCOMSTARTER_DEVICE_TYPE)) {
					String currentVersion;
					currentVersion = monitoringProperties.getProperty(NAMESPACE_UPDATERSERVICE_DEVICE_TYPE_VERSION, deviceType);
					int updateType = getUpdateVersionType(currentVersion, newVersion);
					switch (updateType) {
					case MAJOR:
						
						break;
					case MINOR:
						
						break;
					case PATCH:
						
						break;
					case IDENTICAL_VERSION:							
						printderp("Our current version of " + deviceType + " (" + currentVersion + ") does not differ from UpdateServer's version (" + newVersion + ").");
						printderp("Will not update " + deviceType + ".");
						continue;
					case DOWNGRADE:
						printderp("Our current version of " + deviceType + " (" + currentVersion + ") is greater than the UpdateServer's version (" + newVersion + ").");
						printderp("Will not update " + deviceType + ".");
						continue;
					case INCOMPATIBLE_VERSION_SCHEME:
						printderp("The version schemes are incompatible. Local version of " + deviceType + " (" + currentVersion + ") is not comparable with UpdateServer's version (" + newVersion + ").");
						printderp("Will not update " + deviceType + ".");
						continue;
					default:
						continue;
					}
					
					// Now we know that we should update this device type. It is time to fetch the update content
					updateState = UpdateState.UPDATING_WAITING_FOR_JAR;
					Command updateContentRequest = getProtocolHandler().findCommand(COMMAND_OUT_UPDATE_CONTENT_REQUEST);
					updateContentRequest.findParam(PARAM_DEVICE_TYPE).setData(deviceType.getBytes());
					updateContentRequest.findParam(PARAM_VERSION).setData(newVersion.getBytes());
					sendTo(conn, getProtocolHandler().findCommand(COMMAND_OUT_BENCHMARK_END));
					sendTo(conn, updateContentRequest);
					printderp("Waiting for update data from Update Server...");
					Command command = getCommandFromBuffer(COMMAND_IN_UPDATE_DATA, MAX_SECONDS_WAIT_FOR_DATA);
					if (command == null || command.getID().equals(COMMAND_IN_ABORT_UPDATE)){
//						abortUpdateStageOne("Timeout when waiting for update data to " + deviceType + " " + newVersion + ". Aborting update!");
//						return;
						printderp("Timeout when waiting for update data to " + deviceType + " " + newVersion + ". Will not update " + deviceType + "!");
						continue;
					}
					sendTo(conn, getProtocolHandler().findCommand(COMMAND_OUT_BENCHMARK_END));
					
					// Extract version info and jar content.
					Param pDeviceType = command.findParam(PARAM_DEVICE_TYPE);
					String receivedDeviceType = toUTF8String(pDeviceType.getData());
					Param pVersion = command.findParam(PARAM_VERSION);
					String receivedVersion = toUTF8String(pVersion.getData());
					printderp("Received update to " + receivedDeviceType + " with version " + receivedVersion);
					
					// Check to see if it matches what we requested
					if (!deviceType.equals(receivedDeviceType) || !newVersion.equals(receivedVersion)) {
						printderp("Expected device type or version did not match received "
								+ "device type or version from UpdateServer. Will not update " + deviceType + "!\n"
								+ "Expected: " + deviceType + " version " + newVersion + "\n"
								+ "Received: " + receivedDeviceType + " version " + receivedVersion);
						continue;
					}
					
					// Save new executable
					Param pContent = command.findParam(PARAM_UPDATE_CONTENT);
					byte[] content = pContent.getData();
					String newExecPath;
					try {
						newExecPath = DeviceList.getConfFolder(deviceType).getNativeURL().replace("file:", "")
								+ "/" + deviceType + "-" + newVersion + ".jar";
					} catch (IOException e1) {
						printderp("Could not access global configuration folder for " + deviceType + ". Will not update " + deviceType + "!");
						continue;
					}
					if (!saveJar(content, newExecPath)) {
						printderp("Could not save jar: " + newExecPath + ". Will not update " + deviceType + "!");
						new File(newExecPath).delete();
						continue;
					}
					printderp("New executable for " + deviceType + " saved to: " + newExecPath);
					
					printderp("We will be updating " + deviceType + " from version (" + currentVersion + ") to version (" + newVersion + ").");
					if (deviceType.equals(PALCOMSTARTER_DEVICE_TYPE)) {
						performPalComStarterUpdate = true;
						palComStarterUpdateDescription = new UpdateDescription(deviceType, newVersion, updateType, newExecPath);
					} else {
						performMonitoredDeviceUpdate = true;
						monitoredDeviceTypesToUpdate.add(new UpdateDescription(deviceType, newVersion, updateType, newExecPath));
					}
				} else {
					printderp("Update's device type " + deviceType + " does not match any of our monitored devices");
				}
			}
			if (performMonitoredDeviceUpdate || performPalComStarterUpdate || performMajorUpdate)
				return true;
			else
				return false;
		}
			
		private void stageOne() {
			updateState = UpdateState.UPDATING_KILLING_CURRENT;

			// Disable monitoring. (Otherwise the current devices would be started again by the monitoring thread),
			// and the monitoring thread will get in the way (for example using the socket threads).
			monitor.disable();
			
			// Perform monitored device update
			long benchmark; // benchmark monitored devices
			MonitoredDevice d = null;
			for (UpdateDescription ud: monitoredDeviceTypesToUpdate) {
				List<MonitoredDevice> md = monitor.getMonitoredDevicesOfType(ud.deviceType);
				for(MonitoredDevice monitoredDevice: md) {
					d = monitoredDevice;
					benchmark = System.currentTimeMillis(); // benchmark monitored devices
					monitor.killMonitoredDevice(d, true);
					// Check that socket is working
					if(!monitor.startNewVersionMonitoredDevice(d, ud.version)) {
						printderp("Could not start " + d.deviceType + " " + d.deviceID + " " + ud.version + ". Will not update " + d.deviceID + "!");
						performMajorUpdate = false;
						// Should be like this instead
//						if (performMajorUpdate) {
//							abortUpdateStageOne("KLKDJF");
//						} else {
//							continue;
//						}
						continue;
					}
					if (!socketSender.sendMsg(UPDATE_PROTOCOL_CHECK_SOCKET, MAX_SECONDS_WAIT_FOR_DEVICE)) {
						printderp("Socket check timeout: Could not send msg to new device. Will not update " + d.deviceID + "!");
						monitor.killMonitoredDevice(d, false);
						performMajorUpdate = true;
						continue;
					}
					benchmark = System.currentTimeMillis() - benchmark; // benchmark monitored devices
					System.out.println("###################################################### Benchmark device downtime: ");
					System.out.println("" + benchmark);
					
					String tmpMsg = socketListener.waitForMsg(UPDATE_PROTOCOL_CHECK_SOCKET_CONFIRM, MAX_SECONDS_WAIT_FOR_DEVICE);
					if (tmpMsg == null || tmpMsg.equals(UPDATE_PROTOCOL_ABORT)) {
						printderp("Socket check timeout: No socket reply from new device. Will not update " + d.deviceID + "!");
						monitor.killMonitoredDevice(d, false);
						performMajorUpdate = true;
						continue;
					}
					
					// Check that device can talk to update server
					updateState = UpdateState.UPDATING_FALLBACK_TIMER_CHECK_UPDATE_SERVER;
					if (!socketSender.sendMsg(UPDATE_PROTOCOL_CHECK_UPDATE_SERVER, MAX_SECONDS_WAIT_FOR_DEVICE)) {
						printderp("Update Server check timeout: Could not send msg to new device. Will not update " + d.deviceID + "!");
						monitor.killMonitoredDevice(d, false);
						performMajorUpdate = true;
						continue;
					}
					if (!socketSender.sendMsg(updateServerDeviceID, MAX_SECONDS_WAIT_FOR_DEVICE)) {
						printderp("Update Server check timeout: Could not send msg to new device. Will not update " + d.deviceID + "!");
						monitor.killMonitoredDevice(d, false);
						performMajorUpdate = true;
						continue;
					}
					tmpMsg = socketListener.waitForMsg(UPDATE_PROTOCOL_CHECK_UPDATE_SERVER_CONFIRM, MAX_SECONDS_WAIT_FOR_DEVICE);
					if (tmpMsg == null  || tmpMsg.equals(UPDATE_PROTOCOL_ABORT)) {
						printderp("Update Server check timeout: No socket reply from new device. Will not update " + d.deviceID + "!");
						monitor.killMonitoredDevice(d, false);
						performMajorUpdate = true;
						continue;
					}
					// Do this for all but the last monitored device. Hold on to the last one a bit longer.
					if (monitoredDeviceTypesToUpdate.indexOf(ud) != (monitoredDeviceTypesToUpdate.size() - 1) || md.indexOf(d) != (md.size() - 1)) {
						if (!socketSender.sendMsg(UPDATE_PROTOCOL_FINISH_DEVICE_STARTUP_CHECK, MAX_SECONDS_WAIT_FOR_DEVICE)) {
							printderp("Update Server check timeout: Could not send msg to new device. Will not update " + d.deviceID + "!");
							monitor.killMonitoredDevice(d, false);
							performMajorUpdate = true;
							continue;
						}
						tmpMsg = socketListener.waitForMsg(UPDATE_PROTOCOL_FINISH_DEVICE_STARTUP_CHECK_ACK, MAX_SECONDS_WAIT_FOR_DEVICE);
						if (tmpMsg == null  || tmpMsg.equals(UPDATE_PROTOCOL_ABORT)) {
							printderp("Update Server check timeout: No socket reply from new device. Will not update " + d.deviceID + "!");
							monitor.killMonitoredDevice(d, false);
							performMajorUpdate = true;
							continue;
						}						
					}
				}
				// All monitored devices with deviceType are working, so we update the current version of deviceType
				printderp("Successfully updated " + ud.deviceType + " to version " + ud.version);
				monitor.setCurrentDeviceTypeVersion(ud.deviceType, ud.version);
				monitoringProperties.setProperty(NAMESPACE_UPDATERSERVICE_DEVICE_TYPE_VERSION, ud.deviceType, ud.version);
			}

			// If we will not update PalComStarter, we can let the last monitored device go. 
			// Otherwise, we need it later for stage two.
			if (performMonitoredDeviceUpdate && !performPalComStarterUpdate) {
				if (!socketSender.sendMsg(UPDATE_PROTOCOL_FINISH_DEVICE_STARTUP_CHECK, MAX_SECONDS_WAIT_FOR_DEVICE)) {
					printderp("Update Server check timeout: Could not send msg to new device. Will not update " + d.deviceID + "!");
					monitor.killMonitoredDevice(d, false);
					performMajorUpdate = false;
					updateState = UpdateState.NONE;
					return;
				}
				String tmpMsg = socketListener.waitForMsg(UPDATE_PROTOCOL_FINISH_DEVICE_STARTUP_CHECK_ACK, MAX_SECONDS_WAIT_FOR_DEVICE);
				if (tmpMsg == null  || tmpMsg.equals(UPDATE_PROTOCOL_ABORT)) {
					printderp("Update Server check timeout: No socket reply from new device. Will not update " + d.deviceID + "!");
					monitor.killMonitoredDevice(d, false);
					performMajorUpdate = false;
					updateState = UpdateState.NONE;
					return;
				}
//				sendTo(conn, getProtocolHandler().findCommand(COMMAND_OUT_BENCHMARK_END)); // benchmark monitored devices
				printderp("Updating process finished.");
				monitor.enable();
				updateState = UpdateState.NONE;
				return;
			}
			
			printderp("We are about to update PalComStarter. Time for update stage two.");
			if (!performMonitoredDeviceUpdate) { // If we did not perform update on some monitored device, 
												 // we need to initiate stage two by palcom messages.
				// It is always possible to communicate with PalCom messages in this case, because monitored 
				// devices need to be updated in the event of a major update.
				d = monitor.initiateStageTwo();
				if (d == null) {
					printderp("Could not get hold of a monitored device to initiate stage two with. Will not update " + PALCOMSTARTER_DEVICE_TYPE + ".");
					performMajorUpdate = false;
					return;
				}
			} else { // ... else, we initiate stage two by Update Protocol messages
				if (!socketSender.sendMsg(UPDATE_PROTOCOL_STAGE_TWO, MAX_SECONDS_WAIT_FOR_DEVICE)) {
					printderp("Socket timeout: Could not send msg to new device. Will not update " + PALCOMSTARTER_DEVICE_TYPE + ".");
					performMajorUpdate = false;
					return;
				}
			}
			// Send version of new PalComStarter (used to update startup script in stage three)
			if (!socketSender.sendMsg(palComStarterUpdateDescription.version, MAX_SECONDS_WAIT_FOR_DEVICE)) {
				printderp("Socket timeout: Could not send msg to new device. Will not update " + PALCOMSTARTER_DEVICE_TYPE + ".");
				performMajorUpdate = false;
				return;
			}
			// Sending the start command for the new version of PalComStarter
			String myJarPath = palComStarterUpdateDescription.pathToExec;
			String myDeviceID = container.getDeviceID().getID();
			String newStartCmd = "java -jar " + myJarPath + " -x " + myDeviceID + " -f " + pathToFS;
			// And add the flag telling PalComStarter to continue with stage three when starting:
			newStartCmd += " -" + PalComStarter.COM_CONTINUE_UPDATE_STAGE_THREE;
			if (!socketSender.sendMsg(newStartCmd, MAX_SECONDS_WAIT_FOR_DEVICE)) {
				printderp("Socket timeout: Could not send msg to new device. Will not update " + PALCOMSTARTER_DEVICE_TYPE + ".");
				performMajorUpdate = false;
				return;
			}
			
			
			// TODO we should send Process objects so that new version of PalComStarter gets them
			
			// Wait for killing blow...
			String tmpMsg = socketListener.waitForMsg(UPDATE_PROTOCOL_KILL, MAX_SECONDS_WAIT_FOR_DEVICE);
			if (tmpMsg == null || tmpMsg.equals(UPDATE_PROTOCOL_ABORT)) {
				abortUpdateStageOne("Socket timeout: No kill reply from new device. Aborting update!");
				return;
			}
			// make sure that we wont be disturbed and that the listening socket is closed before moving on
			updateState = UpdateState.UPDATING_DONT_DISTURB;
			socketListener.closeSocket();
			if (!socketSender.sendMsg(UPDATE_PROTOCOL_KILL_ACK, MAX_SECONDS_WAIT_FOR_DEVICE)) {
				abortUpdateStageOne("Socket timeout: Kill ack not received by new device. Aborting update!");
			}
			printderp("UpdateStageOne Thread done. Thread killed.");
			stopDevice();
		}
		
		public void abortUpdateStageOne(String message) { // TODO
			printderp(message);
			// kill any device running new version
			
			// reopen socket if it is closed
			
			// delete jars
			
			// updateAborted++;
			
			updateState = UpdateState.NONE;
			monitor.enable();
			
			// restart current version monitored devices if offline
		}
		
		private int getUpdateVersionType(String currentVersion, String newVersion) {
			String[] splitCurrentVersion = currentVersion.split("\\.");
			String[] splitNewVersion = newVersion.split("\\.");
			if (splitCurrentVersion.length != splitNewVersion.length || splitCurrentVersion.length != 3)
				return INCOMPATIBLE_VERSION_SCHEME;
			int currentMajor = Integer.parseInt(splitCurrentVersion[0]);
			int newMajor = Integer.parseInt(splitNewVersion[0]);
			if (newMajor > currentMajor) {
				return MAJOR;
			} else if (newMajor < currentMajor) {
				return DOWNGRADE;
			}
			int currentMinor = Integer.parseInt(splitCurrentVersion[1]);
			int newMinor = Integer.parseInt(splitNewVersion[1]);
			if (newMinor > currentMinor) {
				return MINOR;
			} else if (newMinor < currentMinor) {
				return DOWNGRADE;
			}
			int currentPatch = Integer.parseInt(splitCurrentVersion[2]);
			int newPatch = Integer.parseInt(splitNewVersion[2]);
			if (newPatch > currentPatch) {
				return PATCH;
			} else if (newPatch < currentPatch) {
				return DOWNGRADE;
			}
			return IDENTICAL_VERSION;
		}
	}
	
	// +----------------------------------------------------------------------------------------------+
	// |                          UpdateStageTwo Thread                                               |
	// +----------------------------------------------------------------------------------------------+
	/**
	 * Thread started by an updated monitored device in order to update the monitor (PalcomStarter).
	 * Similar to UpdateMonitoredDevices Thread.
	 */
	private class UpdateStageTwoThread extends Thread {
		private static final int MAX_SECONDS_WAIT_FOR_DEVICE = 10;		
		private Process p = null;
		private String newPalComStarterCommand;
		private String currentPalComStarterCommand;
		private String newPalComStarterVersion;
		
		@Override
		public void run() {
			printderp("Starting UpdateStageTwo Thread");
			// Setting some variables needed later in the process and in case of emergency abort			
			if(!stageTwoPreparations()) {
				printderp("Will not be able to perform stage two. Aborting update!");
				return;
			}

			try {
				stageTwo();
			} catch (Throwable t) {
				// If stage one could not be completed due to any reason, such as outOfMemoryError, we try to abort gracefully
				abortUpdateStageTwo("Update Stage One received unknown error/exception. Trying to abort gracefully. Message:\n" + t.getMessage());
			}	
		}
		
		private boolean stageTwoPreparations() {
			try {
				currentPalComStarterCommand = toUTF8String(HostFileSystems.getGlobalRoot().getFile("startupscript").getContents());
			} catch (IOException e) {
				printderp("Could not access startup script.");
				return false;
			}
			
			newPalComStarterVersion = socketListener.getMsg();
			// We need to know how to start the current PalcomStarter in case of error.
			newPalComStarterCommand = socketListener.getMsg();
			
			// Send kill to the current PalcomStarter
			socketSender.sendMsg(UPDATE_PROTOCOL_KILL, SocketSender.TRY_FOREVER);
			
			// Wait for PalComStarter to ACK our kill
			socketListener.waitForMsg(UPDATE_PROTOCOL_KILL_ACK , SocketListenerThread.WAIT_FOREVER);
			return true;
		}
		
		private void stageTwo() {
			// Now we are in charge of the update process (so it is also our duty to abort if something goes wrong)
			
			// Start new PalcomStarter
			String[] arguments = newPalComStarterCommand.split(" ");
			String debug = "Going to start new version PalComStarter with:";
			for(String s: arguments)
				debug += " " + s;
			printderp(debug);
			ProcessBuilder pb = new ProcessBuilder(arguments);
			pb.inheritIO();
			try {
				printderp("Starting new PalComStarter...");
				p = pb.start();
			} catch (IOException e) {
				abortUpdateStageTwo("Could not start new PalcomStarter. Aborting update!");
			}
			
			// check that communication via socket is working
			if (!socketSender.sendMsg(UPDATE_PROTOCOL_CHECK_SOCKET, MAX_SECONDS_WAIT_FOR_DEVICE)) {
				abortUpdateStageTwo("Socket check timeout: Could not send msg to new device. Aborting update!");
			}
			String tmpMsg;
			tmpMsg = socketListener.waitForMsg(UPDATE_PROTOCOL_CHECK_SOCKET_CONFIRM, MAX_SECONDS_WAIT_FOR_DEVICE);
			if (tmpMsg == null || tmpMsg.equals(UPDATE_PROTOCOL_ABORT)) {
				abortUpdateStageTwo("Socket check timeout: No socket reply from new device. Aborting update!");
			}
			
			// check that palcomStarter can talk to update server
			if (!socketSender.sendMsg(UPDATE_PROTOCOL_CHECK_UPDATE_SERVER, MAX_SECONDS_WAIT_FOR_DEVICE)) {
				abortUpdateStageTwo("Update Server check timeout: Could not send msg to new device. Aborting update!");
			}
			
			tmpMsg = socketListener.waitForMsg(UPDATE_PROTOCOL_CHECK_UPDATE_SERVER_CONFIRM, MAX_SECONDS_WAIT_FOR_DEVICE);
			if (tmpMsg == null || tmpMsg.equals(UPDATE_PROTOCOL_ABORT)) {
				abortUpdateStageTwo("Update Server check timeout: No socket reply from new device. Aborting update!");
			}
			printderp("PalcomStarter can communicate.");
			
			// PalcomStarter can communicate both by socket and to update server. Time to finish!
			
			if (!socketSender.sendMsg(newPalComStarterVersion, MAX_SECONDS_WAIT_FOR_DEVICE)) {
				abortUpdateStageTwo("Finish stage two timeout: Could not send msg to new device. Aborting update!");
			}
			
			tmpMsg = socketListener.waitForMsg(UPDATE_PROTOCOL_FINISH_STAGE_TWO, MAX_SECONDS_WAIT_FOR_DEVICE);
			if (tmpMsg == null || tmpMsg.equals(UPDATE_PROTOCOL_ABORT)) {
				abortUpdateStageTwo("Finish stage two timeout: Did not receive reply from new device. Aborting update!");
			}
			// make sure that we wont be disturbed and that the listening socket is closed before moving on
			updateState = UpdateState.UPDATING_DONT_DISTURB;
			socketListener.closeSocket();
			if (!socketSender.sendMsg(UPDATE_PROTOCOL_FINISH_STAGE_TWO_ACK, MAX_SECONDS_WAIT_FOR_DEVICE)) {
				abortUpdateStageTwo("Socket timeout: Kill ack not received by new device. Aborting update!");
			}
			
			printderp("UpdateStageTwo Thread is done.");
			updateState = UpdateState.NONE;
		}
		
		public void abortUpdateStageTwo(String message) { // TODO
			printderp(message);
			if (p != null) {
				p.destroyForcibly();
			}
			// Set updateAborted variable so current PalComStarter knows that update failed
			try {
				if (monitoringProperties == null) {
					monitoringProperties = new DeviceProperties(new DeviceID("monitoring"), HostFileSystems.getGlobalRoot(), null, "Monitoring properties. Generated " + new Date());
				}
				String tmp = monitoringProperties.getProperty(NAMESPACE_UPDATERSERVICE_GENERAL, KEY_UPDATE_ABORTED);
				if (tmp != null) {
					int nbrOfTimesAborted = Integer.valueOf(tmp);
					nbrOfTimesAborted++;
					monitoringProperties.setProperty(NAMESPACE_UPDATERSERVICE_GENERAL, KEY_UPDATE_ABORTED, Integer.toString(nbrOfTimesAborted));
				} else {
					monitoringProperties.setProperty(NAMESPACE_UPDATERSERVICE_GENERAL, KEY_UPDATE_ABORTED, "1");
				}
			} catch (IOException e) {
				printderp("Could not tell current PalComStarter that update was aborted.");
			}
			// Start current PalComStarter
			ProcessBuilder pb = new ProcessBuilder(currentPalComStarterCommand.split(" "));
			pb.inheritIO();
			try {
				pb.start();
			} catch (IOException e1) {
				e1.printStackTrace();
				printderp("MAJOR ERROR! Could not start current PalcomStarter. Hoping that the OS will start it later via the startup script...");
				System.exit(0);
			}
			stopDevice();
		}
	}
	
	// +----------------------------------------------------------------------------------------------+
	// |                          UpdateStageThree Thread                                             |
	// +----------------------------------------------------------------------------------------------+
	
	private class UpdateStageThreeThread extends Thread {
		private String currentPalcomStarterCommand;
//		private String currentPathToFS;
		private Writable writableConnToUpdateServer;
		private String deviceID;
//		private String basePath;
//		private String newVersion;
		private String startupScriptURL;
		private String newStartupCommand;
		private File startupScriptBackup;
		private File startupScript;
		private String newVersion;

		@Override
		public void run() {
			printderp("UpdateStageThree Thread started.");
			// Setting some variables needed later in the process and in case of emergency abort			
			if (!stageThreePreparations()) {
				printderp("Will not be able to perform stage three. Aborting update!");
				
				return;
			}

			try {
				stageThree();
			} catch (Throwable t) {
				// If stage one could not be completed due to any reason, such as outOfMemoryError, we try to abort gracefully
				abortUpdateStageThree("Update Stage One received unknown error/exception. Trying to abort gracefully. Message:\n" + t.getMessage());
			}	
		}
		
		private boolean stageThreePreparations() {			
			deviceID = container.getDeviceID().getID();

			socketListener.waitForMsg(UPDATE_PROTOCOL_CHECK_SOCKET, SocketListenerThread.WAIT_FOREVER);
			socketSender.sendMsg(UPDATE_PROTOCOL_CHECK_SOCKET_CONFIRM, SocketSender.TRY_FOREVER);
			socketListener.waitForMsg(UPDATE_PROTOCOL_CHECK_UPDATE_SERVER, SocketListenerThread.WAIT_FOREVER);
			if(updateServerDeviceID == null) { // updateServerDeviceID is set in UpdaterService's constructor
				printderp("Could not find UpdateServer's deviceID in monitoring.properties: " + NAMESPACE_UPDATERSERVICE_GENERAL + "@" + KEY_UPDATE_SERVER_DEVICE_ID);
				printderp("Exiting.");
				stopDevice();
			}
			
			// now we try to communicate with the updateServer
			writableConnToUpdateServer = getWritableConnectionToService(new DeviceID(updateServerDeviceID), UpdateDistributionService.SERVICE_NAME, -1);
			
			// request response from update server in order to test Palcom tunnel/communication
			Command confirmRequestCmd = getProtocolHandler().findCommand(COMMAND_OUT_CHECK_UPDATE_SERVER);
			printderp("Sending confirmmation request to update server");
			sendTo(writableConnToUpdateServer, confirmRequestCmd);
			
			printderp("Waiting for confirmation from update server.");
			getCommandFromBuffer(COMMAND_IN_CHECK_UPDATE_SERVER_CONFIRM);

			printderp("Got response from update server!");
			socketSender.sendMsg(UPDATE_PROTOCOL_CHECK_UPDATE_SERVER_CONFIRM, SocketSender.TRY_FOREVER);
			
			newVersion = socketListener.getMsg();
			
			// Make sure that we can write to the startupscript and to make a backup
			try {
				startupScriptURL = HostFileSystems.getGlobalRoot().getFile("startupscript").getNativeURL().replace("file:", "");
			} catch (IOException e) {
				printderp("Could not get startupScript URL.");
				return false;
			}
			startupScript = new File(startupScriptURL);
			if (!startupScript.isFile()) {
				printderp("StartupScript is not a file.");
				return false;
			}
			if (!startupScript.canRead()) {
				printderp("StartupScript is not readable.");
				return false;
			}
			if (!startupScript.canWrite()) {
				printderp("StartupScript is not writable.");
				return false;
			}
			
			// Prepare new startup script content
			String pathToFS;
			try {
				pathToFS = HostFileSystems.getGlobalRoot().getURL().replace("/PalcomFilesystem/global", "").replace("file:", "");
			} catch (IOException e) {
				printderp("Could not get path to PalcomFilesystem.");
				return false;
			}
			String pathToExec;
			try {
				pathToExec = DeviceList.getConfFolder(PALCOMSTARTER_DEVICE_TYPE).getNativeURL().replace("file:", "");
			} catch (IOException e) {
				printderp("Could not get path to executable.");
				return false;
			}
			pathToExec += PALCOMSTARTER_DEVICE_TYPE + "-" + newVersion + ".jar";
			newStartupCommand = "java -jar " + pathToExec + " -x " + deviceID + " -f " + pathToFS;
			
			// Creating backup
			startupScriptBackup = new File(startupScriptURL + ".bak");
			if (startupScriptBackup.exists())
				startupScriptBackup.delete();
			try {
				Files.copy(startupScript.toPath(), startupScriptBackup.toPath());
			} catch (IOException e1) {
				printderp("Could not create backup of startup script file.");
				return false;
			}
			
			// finish stage two
			socketSender.sendMsg(UPDATE_PROTOCOL_FINISH_STAGE_TWO, SocketListenerThread.WAIT_FOREVER);
			
			// Wait for other device to ACK our finish
			socketListener.waitForMsg(UPDATE_PROTOCOL_FINISH_STAGE_TWO_ACK, SocketListenerThread.WAIT_FOREVER);
			return true;
		}
		
		private void stageThree() {
			// Now we are in charge of the update process
			printderp("Updating host's startup script");
			
			// Write new content to startup script
			FileWriter fw = null;
			try {
				fw = new FileWriter(startupScript, false);
			} catch (IOException e) {
				// Can not happen! Already checked in preparations!
				e.printStackTrace();
				fw = null;
				// Abort if this happens, becase something must be very wrong
				abortUpdateStageThree("StartupScript file cannot be opened for unknown reason. Aborting update!");
			}
			try {
				fw.write(newStartupCommand);
				fw.flush();
				fw.close();
			} catch (IOException e) {
				// Restore startupScript backup
				startupScript.delete();
				if (!startupScriptBackup.renameTo(startupScript)) {							
					printderp("MAJOR ERROR! Could not restore startupScript backup!");
				}
				abortUpdateStageThree("Error while updating startup script. Aborting update!");
			}
			// Remove startupScript backup
			startupScriptBackup.delete();
			
//			}
			// Remove update aborted counter so it won't effect next updating process
			monitoringProperties.removeProperty(NAMESPACE_UPDATERSERVICE_GENERAL, KEY_UPDATE_ABORTED);
		
			// TODO Delete old version executables?
			
			monitoringProperties.setProperty(NAMESPACE_UPDATERSERVICE_DEVICE_TYPE_VERSION, PALCOMSTARTER_DEVICE_TYPE, newVersion);
			sendTo(writableConnToUpdateServer, getProtocolHandler().findCommand(COMMAND_OUT_BENCHMARK_END)); // benchmark
			printderp("Updating done");
		}
		
		public void abortUpdateStageThree(String message) {
			printderp(message);
			// Set updateAborted variable so current PalComStarter knows that update failed
			String tmp = monitoringProperties.getProperty(NAMESPACE_UPDATERSERVICE_GENERAL, KEY_UPDATE_ABORTED);
			if (tmp != null) {
				int nbrOfTimesAborted = Integer.valueOf(tmp);
				nbrOfTimesAborted++;
				monitoringProperties.setProperty(NAMESPACE_UPDATERSERVICE_GENERAL, KEY_UPDATE_ABORTED, Integer.toString(nbrOfTimesAborted));
			} else {
				monitoringProperties.setProperty(NAMESPACE_UPDATERSERVICE_GENERAL, KEY_UPDATE_ABORTED, "1");
			}
			// Start current PalComStarter
			ProcessBuilder pb = new ProcessBuilder(currentPalcomStarterCommand.split(" "));
			pb.inheritIO();
			try {
				pb.start();
			} catch (IOException e1) {
				e1.printStackTrace();
				printderp("MAJOR ERROR! Could not start current PalcomStarter. Hoping that the OS will start it later via the startup script...");
				System.exit(0);
			}
			// TODO Tell JVM to delete new filesystem when it shuts down
//			new File(basePath).deleteOnExit();
//			deleteFilesystem(basePath, true);
//			stopDevice();
		}
	}
	
	// +----------------------------------------------------------------------------------------------+
	// |                          Monitoring Thread                                                   |
	// +----------------------------------------------------------------------------------------------+
	/**
	 * Monitors specified devices. Uses discovery mechanism to see 
	 * if monitored devices are up or down. If they are down, and monitoring
	 * is enabled, they are restarted.
	 */
	private class MonitoringThread extends Thread implements ResourceListener {
		private static final int MAX_SECONDS_WAIT_FOR_DEVICE = 5;
		private static final int RECENTLY_STARTED_WAIT_SEC = 10;
		private ArrayList<MonitoredDevice> monitoredDevices;
		private HashMap<String,String> typeToVersionMap;
		private Semaphore disableMonitorLock;
		private Semaphore checkListSemaphore;
		private Semaphore doingStuffLock;
		private boolean halt = false;
		private boolean monitoringEnabled = true;
		Timer timer;
		private ConcurrentLinkedQueue<MonitoredDevice> checkList;

		public MonitoringThread() {
			monitoredDevices = new ArrayList<MonitoredDevice>();
			disableMonitorLock = new Semaphore(1);
			checkListSemaphore = new Semaphore(0);
			doingStuffLock = new Semaphore(1);
			timer = new Timer();
			checkList = new ConcurrentLinkedQueue<MonitoredDevice>();
			typeToVersionMap = new HashMap<String,String>();
		}
		

		public void setCurrentDeviceTypeVersion(String deviceType, String deviceVersion) {
			typeToVersionMap.put(deviceType, deviceVersion);
		}

		public List<MonitoredDevice> getMonitoredDevicesOfType(String deviceType) {
			List<MonitoredDevice> mdList = new ArrayList<MonitoredDevice>();
			for (MonitoredDevice d: monitoredDevices)
				if (d.deviceType.equals(deviceType))
					mdList.add(d);
			return mdList;
		}
		
		public List<String> getMonitoredDeviceTypes() {
			List<String> deviceTypes = new LinkedList<String>();
			for (MonitoredDevice d: monitoredDevices) {
				if (!deviceTypes.contains(d.deviceType)) {
					deviceTypes.add(d.deviceType);
				}
			}
			return deviceTypes;
		}

		public boolean monitorsDeviceType(String deviceType) {
			return typeToVersionMap.containsKey(deviceType);
		}

		public String getListOfMonitoredDevices() {
			String list = "";
			int i = 0;
			for (MonitoredDevice d: monitoredDevices) {
				list += "index=" + i++ + " ID=" + d.deviceID + " type=" + d.deviceType + " ";
			}
			return list;
		}

		public void stopThread() {
			if (!halt) {
				halt = true;
				enable(); // Nothing bad happens if monitoring is already enabled
				checkListSemaphore.release();				
			}
		}

		public void addNewMonitoredDevice(String deviceID, String instanceName, String typeOfDevice, String monitoredDeviceVersion) {
			DeviceID dID = new DeviceID(deviceID);
			PalcomDevice palcomDevice = container.getDiscoveryManager().getDevice(dID);
			palcomDevice.addListener(this);
			typeToVersionMap.put(typeOfDevice, monitoredDeviceVersion);
			MonitoredDevice monitoredDevice = new MonitoredDevice(dID, typeOfDevice, palcomDevice);
			monitoredDevices.add(monitoredDevice);
			printderp("Adding device to monitor:");
			printderp("Name: " + instanceName);
			printderp("ID: " + deviceID);
			printderp("Device type: " + typeOfDevice);
			printderp("");
		}

		/**
		 * Blocking until monitoring thread has stopped.
		 */
		public void disable() {
			if (monitoringEnabled) {
				printderp("Disabling monitoring.");
				disableMonitorLock.acquireUninterruptibly();
				// wait for the monitoring thread to finish what it is doing
				if(!doingStuffLock.tryAcquire()){
					printderp("Waiting for monitoring thread to finish what it is doing...");
					doingStuffLock.acquireUninterruptibly();
					printderp("Monitoring thread has finished doing its stuff. Monitoring is now disabled.");
				}
				doingStuffLock.release();		
				monitoringEnabled = false;
			}
		}
		
		public void enable() {
			if (!monitoringEnabled) {
				printderp("Enabling monitoring.");
				disableMonitorLock.release();
				monitoringEnabled = true;
			}
		}

		public void killMonitoredDeviceByIndex(int index, boolean b) {
			if (index >= 0 && index < monitoredDevices.size())
				killMonitoredDevice(monitoredDevices.get(index), b);
		}
		
		public MonitoredDevice initiateStageTwo() {
			for (MonitoredDevice d: monitoredDevices) {
				Command cmd = getProtocolHandler().findCommand(COMMAND_IN_INITIATE_STAGE_TWO);
				if(sendCommandToMonitoredDevice(d, cmd)) {
					return d;
				}
			}
			return null;
		}
		
		private boolean sendCommandToMonitoredDevice(MonitoredDevice monitoredDevice, Command cmd) {
			if (monitoredDevice.conn == null){
				printderp("No connection registered to device " + monitoredDevice.deviceID);
				return false;
			}
			int status = sendTo(monitoredDevice.conn, cmd);
			switch (status) {
			case SEND_BUFFER_FULL:
				printderp("Send buffer full");
				return false;
			case SEND_ERROR:
				printderp("Send error");
				return false;
			case SEND_OK:
				printderp("Successfully sent command " + cmd.getID() + " to " + monitoredDevice.deviceID);
				return true;
			default:
				break;
			}
			return false;
		}

		public synchronized void killMonitoredDevice(MonitoredDevice monitoredDevice, boolean startGentle) {
			if (startGentle) {
				Command killCmd = getProtocolHandler().findCommand(COMMAND_OUT_KILL);
				if(!sendCommandToMonitoredDevice(monitoredDevice, killCmd)) {
					printderp("Could not send " + killCmd.getID() + " to " + monitoredDevice.deviceID);
				}
			}
			if(monitoredDevice.p != null) {
				printderp("Trying to kill monitored device forcibly...");
				monitoredDevice.p.destroyForcibly();
				try {
					monitoredDevice.p.waitFor();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				printderp("Device killed.");
				monitoredDevice.p = null;
			}
		}
		
		public void killAllMonitoredDevices(boolean startGentle) {
			for (MonitoredDevice d: monitoredDevices)
				killMonitoredDevice(d, startGentle);
		}

		public void startMonitoredDeviceByIndex(int index) {
			if (index >= 0 && index < monitoredDevices.size()) {
				MonitoredDevice d = monitoredDevices.get(index);
				if (startMonitoredDevice(d))
					performMonitoredDeviceStartupCheck(d);
			}
		}
		
		public synchronized boolean startMonitoredDevice(MonitoredDevice monitoredDevice) {
			String pathToJar = getMonitoredDevicePathToJar(monitoredDevice);
			return startMonitoredDeviceHelper(monitoredDevice, pathToJar);
		}

		public boolean startNewVersionMonitoredDevice(MonitoredDevice monitoredDevice, String version) {
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
		
		public boolean startMonitoredDeviceHelper(MonitoredDevice monitoredDevice, String pathToJar) {
			String pathToFS;
			try {
				pathToFS = HostFileSystems.getGlobalRoot().getURL().replace("/PalcomFilesystem/global", "").replace("file:", "");
			} catch (IOException e1) {
				return false;
			}
			String[] arguments = {"java", "-jar", pathToJar, "-x", monitoredDevice.deviceID.getID(), "-f", pathToFS};
			String msg = "Starting monitored device with:";
			for(String s: arguments)
				msg += " " + s;
			printderp(msg);
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


		public void restartAllMonitoredDevices() {
			for (MonitoredDevice d: monitoredDevices)
				restartMonitoredDevice(d);
		}
		
		public void restartMonitoredDeviceByIndex(int index) {
			if (index >= 0 && index < monitoredDevices.size())
				restartMonitoredDevice(monitoredDevices.get(index));
		}
		
		private void restartMonitoredDevice(MonitoredDevice d) {
			// Kill first, in case it is a zombie. It was not found by PalCom discovery so there is
//			// no idea to try to kill it via PalCom commands. Kill it with brute force.
			killMonitoredDevice(d, false);
			
			if (!startMonitoredDevice(d)) {
				printderp("Could not start monitored device: " + d.deviceID + ". Will try again in " + RECENTLY_STARTED_WAIT_SEC + "s.");
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
				printderp("Monitored device " + d.deviceID + " failed startup check. Will try again in " + RECENTLY_STARTED_WAIT_SEC + "s.");
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
				printderp("Child started and startup check finished.");
			}
		}
		
		private void checkMonitoredDevice(MonitoredDevice d) {
			PalcomDevice pd = d.palcomDevice;
			if(!pd.isReady()) {
				printderp("Could not find " + d.deviceID.getID() + " on network.");
				long timeDiff = d.recentlyStartedDelay - System.currentTimeMillis();
				if (timeDiff > 0) {
					printderp("Recently started " + d.deviceID.getID() + ". Waiting " + timeDiff + "ms before trying to start it again.");
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
					printderp("It will be restarted.");
					restartMonitoredDevice(d);					
				}
			} else {
				printderp("Device " + pd.getDeviceID().getID() + " is up and running. It does not need to be restarted.");
			}
		}
		
		private void checkAllMonitoredDevices() {
			for (MonitoredDevice d: monitoredDevices) {
				checkMonitoredDevice(d);
			}
		}
		
		private boolean performMonitoredDeviceStartupCheck(MonitoredDevice d) {
			if (!socketSender.sendMsg(UPDATE_PROTOCOL_CHECK_SOCKET, MAX_SECONDS_WAIT_FOR_DEVICE)) {
				printderp("Send msg timeout: Could not send msg to monitored device. Shutting down device again.");
				killMonitoredDevice(d, false);
				return false;
			}
			if (socketListener.waitForMsg(UPDATE_PROTOCOL_CHECK_SOCKET_CONFIRM, MAX_SECONDS_WAIT_FOR_DEVICE) == null) {
				printderp("Wait for msg timeout: No socket response from monitored device. Shutting down device again.");
				killMonitoredDevice(d, false);
				return false;
			}
			if (!socketSender.sendMsg(UPDATE_PROTOCOL_FINISH_DEVICE_STARTUP_CHECK, MAX_SECONDS_WAIT_FOR_DEVICE)) {
				printderp("Send msg timeout: Could not send msg to monitored device. Shutting down device again.");
				killMonitoredDevice(d, false);
				return false;
			}
			if (socketListener.waitForMsg(UPDATE_PROTOCOL_FINISH_DEVICE_STARTUP_CHECK_ACK, MAX_SECONDS_WAIT_FOR_DEVICE) == null) {
				printderp("Wait for msg timeout: No socket response from monitored device. Shutting down device again.");
				killMonitoredDevice(d, false);
				return false;
			}
			printderp("Startup check finished.");
			return true;
		}

		@Override
		public void run() {
			printderp("Monitoring Thread started. Doing startup check.");
			doingStuffLock.acquireUninterruptibly();
			checkAllMonitoredDevices();
			doingStuffLock.release();
			while(true) {	
				try {
					checkListSemaphore.acquire(); // wait for something to check in the checklist
				} catch (InterruptedException e1) {/* do nothing */}
				if(!disableMonitorLock.tryAcquire()){ // there is something to check, but monitoring may be disabled
					printderp("Monitoring is temporarily disabled.");
					disableMonitorLock.acquireUninterruptibly();
					printderp("Monitoring is enabled.");
				}
				disableMonitorLock.release();
				if(halt) {
					printderp("Monitoring Thread stopped.");
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
				printderp("Unvailable: " + dp.getDeviceID().getID());
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
				printderp("Unavailable: " + resource.toString());				
			}
		}

		@Override
		public void available(Resource resource) {
			if (resource instanceof DeviceProxy) {
				DeviceProxy dp = (DeviceProxy) resource;
				printderp("Available: " + dp.getDeviceID().getID());
				for (MonitoredDevice md: monitoredDevices) {
					if (dp.getDeviceID().getID().equals(md.deviceID.getID())) {
						md.conn = getWritableConnectionToService(md.deviceID, UpdaterService.SERVICE_NAME, MAX_SECONDS_WAIT_FOR_DEVICE);
						break;
					}
				}
			} else {
				printderp("Available: " + resource.toString());				
			}
		}
		@Override
		public void resourceChanged(Resource resource) { /* We do not care */}
	}
	
	/**
	 * Keeps track of the connection to the update server if it goes down and goes up again.
	 * @author splushii
	 *
	 */
	private class UpdateServerConnectionListener implements ResourceListener {
		private static final int MAX_SECONDS_WAIT_FOR_CONNECTION = 5;
		private Writable writableConnToUpdateServer = null;
		private DeviceID updateServerDID;
		private PalcomDevice updateServerDevice;
		
		public UpdateServerConnectionListener() {
			if (updateServerDeviceID == null) {
				printderp("No Device ID to UpdateServer in configuration: " + NAMESPACE_UPDATERSERVICE_GENERAL + "@" + KEY_UPDATE_SERVER_DEVICE_ID);
				return;
			}
			updateServerDID = new DeviceID(updateServerDeviceID);
			updateServerDevice = container.getDiscoveryManager().getDevice(updateServerDID);
		}
		
		public void addUpdateServerListener() {
			updateServerDevice.addListener(this);
		}
		
		public void removeUpdateServerListener() {
			updateServerDevice.removeListener(this);
		}
		
		private boolean checkUpdateServer() {
			// Initiate connection with Update Server			
			if (writableConnToUpdateServer == null) {
				writableConnToUpdateServer = getWritableConnectionToService(updateServerDID, UpdateDistributionService.SERVICE_NAME, MAX_SECONDS_WAIT_FOR_CONNECTION);
				if (writableConnToUpdateServer == null) {
					printderp("Could not establish connection to UpdateServer. Will not be able to receive updates.");
					return false;
				}
			}
			Connection connToUpdateServer = (Connection) writableConnToUpdateServer;
			if(!connToUpdateServer.isOpen()) {
				// The connection is closed so we need to establish a new one.
				writableConnToUpdateServer = getWritableConnectionToService(updateServerDID, UpdateDistributionService.SERVICE_NAME, MAX_SECONDS_WAIT_FOR_CONNECTION);
				if (writableConnToUpdateServer == null) {
					printderp("Could not establish connection to UpdateServer. Will not be able to receive updates.");
					return false;
				}
			}
			printderp("Writable connection established to UpdateServer. We will now be able to receive updates.");
			return true;
		}
		
		public void checkLatestVersion() {
			printderp("Checking latest version.");
			if (checkUpdateServer()) {
				Command cmd = getProtocolHandler().findCommand(COMMAND_OUT_CHECK_LATEST_VERSION); 
				String deviceTypes = PALCOMSTARTER_DEVICE_TYPE;
				for (String deviceType: monitor.getMonitoredDeviceTypes()) {
					deviceTypes += PARAM_VALUE_SEPARATOR + deviceType;
				}
				cmd.findParam(PARAM_DEVICE_TYPE).setData(deviceTypes.getBytes());
				sendTo(writableConnToUpdateServer, cmd);				
			}
		}

		@Override
		public void available(Resource resource) {
			printderp("UpdateServer available again!");	
			checkLatestVersion();
		}

		@Override
		public void unavailable(Resource resource) {
			printderp("UpdateServer unavailable :(");
//			writableConnToUpdateServer = null;
		}

		@Override
		public void resourceChanged(Resource r) {/* We do not care */}
	}

	/**
	 *
	 * @param deviceID
	 * @param serviceName
	 * @param maxSecondsToWait, if -1 it will try forever
	 * @return
	 */
	public Writable getWritableConnectionToService(DeviceID deviceID, String serviceName, int maxSecondsToWait) {
		long timeBetweenReadyChecks = 100; // milliseconds
		Writable writableConn = null;
		PalcomDevice pd = container.getDiscoveryManager().getDevice(deviceID);
		long timeToStop = System.currentTimeMillis() + maxSecondsToWait*1000;
		while (!pd.isReady()) {
//			printderp("UpdateServer device not ready. Trying again in " + timeBetweenReadyChecks + "ms." );
			if (maxSecondsToWait != -1 && System.currentTimeMillis() > timeToStop) {
				return null;
			}
			try {
				Thread.sleep(timeBetweenReadyChecks);
			} catch (InterruptedException e) {
				return null;
			}
		}
		printderp("Device with ID " + deviceID + " is ready.");
		PalcomServiceList psl = pd.getServiceList();
		PalcomService ps = null;
		try {
			for (int i = 0; i < psl.getNumService(); ++i) {
				ps = (PalcomService) psl.getService(i);
				if(ps.getName().equals(serviceName)) {
					printderp("Service with name " + serviceName + " found.");
					break;
				} else {
					ps = null;
				}
			}
		} catch (ResourceException e) {/* handled below */}
		if (ps == null) {
			printderp("Did not find service with name " + serviceName + " on device with ID " + deviceID + ".");
			return null;
		}
		while (!ps.isReady()) {
//			printderp(serviceName + " is not ready. Trying again in " + timeBetweenReadyChecks + "ms." );
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
				printderp("Successfully connected to " + serviceName + " on " + deviceID);
				if (conn instanceof Writable) {
					writableConn = (Writable) conn;
				}
			} catch (IllegalStateException e) {
				printderp("Connection to " + serviceName + " is already open.");
			} catch (NoSuchDeviceException e) {
				printderp("Device " + deviceID + "cannot be found.");
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
}
