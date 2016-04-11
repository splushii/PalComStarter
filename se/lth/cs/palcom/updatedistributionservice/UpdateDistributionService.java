package se.lth.cs.palcom.updatedistributionservice;

import java.util.HashMap;
import ist.palcom.resource.descriptor.Command;
import ist.palcom.resource.descriptor.DeviceID;
import ist.palcom.resource.descriptor.PRDService;
import ist.palcom.resource.descriptor.ServiceID;
import se.lth.cs.palcom.communication.connection.Readable;
import se.lth.cs.palcom.communication.connection.Writable;
import se.lth.cs.palcom.device.AbstractDevice;
import se.lth.cs.palcom.logging.Logger;
import se.lth.cs.palcom.service.AbstractSimpleService;
import se.lth.cs.palcom.service.ServiceTools;
import se.lth.cs.palcom.service.command.CommandServiceProtocol;
import se.lth.cs.palcom.service.distribution.UnicastDistribution;
import se.lth.cs.palcom.updaterservice.UpdaterService;

/** 
 * Keeps track of updates and distributes them to devices running {@link UpdaterService} in monitoring mode (PalComStarters)
 * @author Christian Hernvall
 *
 */
public class UpdateDistributionService extends AbstractSimpleService {
	public static final DeviceID CREATOR = new DeviceID("X:mojo");
	public static final ServiceID SERVICE_VERSION = new ServiceID(CREATOR, "UPD1.0.0", CREATOR, "UPD1.0.0");
	public static final String SERVICE_NAME = "UpdateDistributionService";
	
	private static final String COMMAND_IN_BROADCAST_UPDATE_SINGLE_DEVICE_TYPE = "broadcast update for single device type";
	private static final String COMMAND_IN_BROADCAST_UPDATE_MULTIPLE_DEVICES = "broadcast update for multiple device types";
	private static final String COMMAND_IN_UPDATE_CONTENT_REQUEST = se.lth.cs.palcom.updaterservice.UpdaterService.COMMAND_OUT_UPDATE_CONTENT_REQUEST;
	private static final String COMMAND_IN_CHECK_UPDATE_SERVER = se.lth.cs.palcom.updaterservice.UpdaterService.COMMAND_OUT_CHECK_UPDATE_SERVER;
	private static final String COMMAND_IN_CHECK_LATEST_VERSION = se.lth.cs.palcom.updaterservice.UpdaterService.COMMAND_OUT_CHECK_LATEST_VERSION;
	private static final String COMMAND_IN_BENCHMARK_END = se.lth.cs.palcom.updaterservice.UpdaterService.COMMAND_OUT_BENCHMARK_END;
	private static final String COMMAND_IN_ADD_JAR = "add jar";
	
	private static final String COMMAND_OUT_UPDATE_DEVICE_TYPES = se.lth.cs.palcom.updaterservice.UpdaterService.COMMAND_IN_UPDATE_DEVICE_TYPES;
	private static final String COMMAND_OUT_UPDATE_DATA = se.lth.cs.palcom.updaterservice.UpdaterService.COMMAND_IN_UPDATE_DATA;
	private static final String COMMAND_OUT_CHECK_UPDATE_SERVER_CONFIRM = se.lth.cs.palcom.updaterservice.UpdaterService.COMMAND_IN_CHECK_UPDATE_SERVER_CONFIRM;
	
	private static final String PARAM_VALUE_SEPARATOR = se.lth.cs.palcom.updaterservice.UpdaterService.PARAM_VALUE_SEPARATOR;
	private static final String PARAM_VERSION = se.lth.cs.palcom.updaterservice.UpdaterService.PARAM_VERSION;
	private static final String PARAM_UPDATE_CONTENT = se.lth.cs.palcom.updaterservice.UpdaterService.PARAM_UPDATE_CONTENT;
	private static final String PARAM_DEVICE_TYPE = se.lth.cs.palcom.updaterservice.UpdaterService.PARAM_DEVICE_TYPE;
	private static final String PARAM_VERSION_ENTRY_UNKNOWN = se.lth.cs.palcom.updaterservice.UpdaterService.PARAM_NO_ENTRY;
	
	private long benchmark;
	// TODO: save updates to disk for consistency between restarts
	// [device folder]/updates/[implementation]/[PalCom version]/[deviceType].[suffix]
	// for example [deviceID]/updates/java/1.2/thething.jar
	HashMap<String, UpdateEntry> deviceTypeToJarMap;
	
	// TODO: Add command to list updates
	
	// TODO: Add command to remove one/all updates
	
	public UpdateDistributionService(AbstractDevice container) {
		this(container, ServiceTools.getNextInstance(SERVICE_VERSION));
	}
	
	public UpdateDistributionService(AbstractDevice container, String instance) {
		super(container, SERVICE_VERSION, "P1", "v0.0.1", "UpdateDistributionService",
				instance, "Distributes updates to PalCom devices",
				new UnicastDistribution(true));
		
		deviceTypeToJarMap = new HashMap<String, UpdateEntry>();
		
		CommandServiceProtocol sp = getProtocolHandler();
		
		Command broadcastSingleUpdateCmd = new Command(COMMAND_IN_BROADCAST_UPDATE_SINGLE_DEVICE_TYPE, "Upload specified update and send information about it to all connected clients.", Command.DIRECTION_IN);
		broadcastSingleUpdateCmd.addParam(PARAM_DEVICE_TYPE, "text/plain");
		broadcastSingleUpdateCmd.addParam(PARAM_VERSION, "text/plain");
		broadcastSingleUpdateCmd.addParam(PARAM_UPDATE_CONTENT, "application/x-jar");
		sp.addCommand(broadcastSingleUpdateCmd);
		
		Command broadcastMultipleUpdateCmd = new Command(COMMAND_IN_BROADCAST_UPDATE_MULTIPLE_DEVICES, "Send information about the latest updates of all device types to all connected clients.", Command.DIRECTION_IN);
		sp.addCommand(broadcastMultipleUpdateCmd);
		
		Command updateContentRequestCmd = new Command(COMMAND_IN_UPDATE_CONTENT_REQUEST, "Update content request.", Command.DIRECTION_IN);
		updateContentRequestCmd.addParam(PARAM_DEVICE_TYPE, "text/plain");
		updateContentRequestCmd.addParam(PARAM_VERSION, "text/plain");
		sp.addCommand(updateContentRequestCmd);
	
		Command checkUpdateServerCmd = new Command(COMMAND_IN_CHECK_UPDATE_SERVER, "Confirmation request from client.", Command.DIRECTION_IN);
		sp.addCommand(checkUpdateServerCmd);
		
		Command latestVersionRequestCmd = new Command(COMMAND_IN_CHECK_LATEST_VERSION, "Latest version request from client.", Command.DIRECTION_IN);
		latestVersionRequestCmd.addParam(PARAM_DEVICE_TYPE, "text/plain");
		sp.addCommand(latestVersionRequestCmd);

		Command updateCmd = new Command(COMMAND_OUT_UPDATE_DEVICE_TYPES, "", Command.DIRECTION_OUT);
		updateCmd.addParam(PARAM_DEVICE_TYPE, "text/plain");
		updateCmd.addParam(PARAM_VERSION, "text/plain");
		sp.addCommand(updateCmd);
		
		Command updateContentCmd = new Command(COMMAND_OUT_UPDATE_DATA, "Reply with content to content request.", Command.DIRECTION_OUT);
		updateContentCmd.addParam(PARAM_DEVICE_TYPE, "text/plain");
		updateContentCmd.addParam(PARAM_VERSION, "text/plain");
		updateContentCmd.addParam(PARAM_UPDATE_CONTENT, "application/x-jar");
		sp.addCommand(updateContentCmd);

		Command confirmReqCmd = new Command(COMMAND_OUT_CHECK_UPDATE_SERVER_CONFIRM, "Confirmation reply to confirmation request.", Command.DIRECTION_OUT);
		sp.addCommand(confirmReqCmd);
		
		Command addJarCmd = new Command(COMMAND_IN_ADD_JAR, "Add jar to database", Command.DIRECTION_IN);
		addJarCmd.addParam(PARAM_DEVICE_TYPE, "text/plain");
		addJarCmd.addParam(PARAM_VERSION, "text/plain");
		addJarCmd.addParam(PARAM_UPDATE_CONTENT, "application/x-jar");
		sp.addCommand(addJarCmd);
		
		Command benchmarkEndCmd = new Command(COMMAND_IN_BENCHMARK_END, "benchmark end", Command.DIRECTION_IN);
		sp.addCommand(benchmarkEndCmd);
	}

	@Override
	protected void invoked(Readable connection, Command command) {
		if (connection instanceof Writable) {
			Writable conn = (Writable) connection;
			if(command.getID().equals(COMMAND_IN_BROADCAST_UPDATE_SINGLE_DEVICE_TYPE)) {
				benchmark = System.currentTimeMillis();
				Logger.log("Benchmarking time to update. Current time: " + benchmark, Logger.CMP_SERVICE, Logger.LEVEL_BULK);
				String deviceType = UpdaterService.toUTF8String(command.findParam(PARAM_DEVICE_TYPE).getData());
				String version = UpdaterService.toUTF8String(command.findParam(PARAM_VERSION).getData());
				byte[] content = command.findParam(PARAM_UPDATE_CONTENT).getData();
				Logger.log("Saving update " + deviceType + "(v" + version + ") and broadcasting update info to devices.", Logger.CMP_SERVICE, Logger.LEVEL_INFO);
				deviceTypeToJarMap.put(deviceType, new UpdateEntry(version, content.clone()));
				announceNewUpdate(new String[] {deviceType}, new String[] {version});
			} else if (command.getID().equals(COMMAND_IN_UPDATE_CONTENT_REQUEST)) {
				String deviceType = UpdaterService.toUTF8String(command.findParam(PARAM_DEVICE_TYPE).getData());
				String version = UpdaterService.toUTF8String(command.findParam(PARAM_VERSION).getData()); // TODO we are not using version
				Logger.log("Replying with update content (v" + version + ") to " + deviceType + ".", Logger.CMP_SERVICE, Logger.LEVEL_INFO);
				replyWithJar(deviceType, conn);
			} else if (command.getID().equals(COMMAND_IN_CHECK_UPDATE_SERVER)) {				
				Logger.log("Replying to a device checking its connection to me.", Logger.CMP_SERVICE, Logger.LEVEL_DEBUG);
				replyToConfirmRequest(conn);
			} else if (command.getID().equals(COMMAND_IN_CHECK_LATEST_VERSION)) {
				Logger.log("Replying with latest version info to device.", Logger.CMP_SERVICE, Logger.LEVEL_INFO);
				String deviceTypes = UpdaterService.toUTF8String(command.findParam(PARAM_DEVICE_TYPE).getData());
				replyWithLatestVersion(conn, deviceTypes);
			} else if (command.getID().equals(COMMAND_IN_BENCHMARK_END)) {
				Logger.log("Got benchmark end command. Current time: " + System.currentTimeMillis(), Logger.CMP_SERVICE, Logger.LEVEL_BULK);
				Logger.log("Difference between start and now: " + (System.currentTimeMillis() - benchmark), Logger.CMP_SERVICE, Logger.LEVEL_BULK);
			} else if (command.getID().equals(COMMAND_IN_ADD_JAR)) {
				String deviceType = UpdaterService.toUTF8String(command.findParam(PARAM_DEVICE_TYPE).getData());
				String version = UpdaterService.toUTF8String(command.findParam(PARAM_VERSION).getData());
				byte[] content = command.findParam(PARAM_UPDATE_CONTENT).getData();
				Logger.log("Adding " + deviceType + " version " + version + " to update database.", Logger.CMP_SERVICE, Logger.LEVEL_INFO);
				deviceTypeToJarMap.put(deviceType, new UpdateEntry(version, content));
			} else if (command.getID().equals(COMMAND_IN_BROADCAST_UPDATE_MULTIPLE_DEVICES)) {
				benchmark = System.currentTimeMillis();
				Logger.log("Benchmarking time to update. Current time: " + benchmark, Logger.CMP_SERVICE, Logger.LEVEL_BULK);
				int size = deviceTypeToJarMap.keySet().size();
				String[] deviceTypes = new String[size];
				String[] versions = new String[size];
				int i = 0;
				String msg = "Broadcasting update for device types:";
				for (String deviceType: deviceTypeToJarMap.keySet()) {
					deviceTypes[i] = deviceType;
					versions[i] = deviceTypeToJarMap.get(deviceType).version;
					msg += " " + deviceType;
					i++;
				}
				Logger.log(msg, Logger.CMP_SERVICE, Logger.LEVEL_INFO);
				announceNewUpdate(deviceTypes, versions);
			}
		}
	}
	
	private void replyWithLatestVersion(Writable conn, String deviceTypes) {
		String[] splitDeviceTypes = deviceTypes.split(PARAM_VALUE_SEPARATOR);
		String versions = null;
		for (String deviceType: splitDeviceTypes) {
			String latestVersion = null;
			if (deviceTypeToJarMap.containsKey(deviceType)) {
				latestVersion = deviceTypeToJarMap.get(deviceType).version;					
			} else {
				latestVersion = PARAM_VERSION_ENTRY_UNKNOWN;
			}
			if (versions != null) {
				versions += PARAM_VALUE_SEPARATOR;
			} else {
				versions = "";
			}
			versions += latestVersion;
		}
		if (versions != null) {
			Command reply = getProtocolHandler().findCommand(COMMAND_OUT_UPDATE_DEVICE_TYPES);
			reply.findParam(PARAM_DEVICE_TYPE).setData(deviceTypes.getBytes());
			reply.findParam(PARAM_VERSION).setData(versions.getBytes());
			sendTo(conn, reply);
		}
	}

	private void replyToConfirmRequest(Writable conn) {
		Command reply = getProtocolHandler().findCommand(COMMAND_OUT_CHECK_UPDATE_SERVER_CONFIRM);
		sendTo(conn, reply);
	}

	private void replyWithJar(String deviceType, Writable conn) { // TODO make it search for the right version
		UpdateEntry updateEntry = deviceTypeToJarMap.get(deviceType);
		String version = updateEntry.version;
		byte[] content = updateEntry.content;
		if (content == null) {
			Logger.log("Could not find jar with version " + version + " for the client.", Logger.CMP_SERVICE, Logger.LEVEL_WARNING);
		} else {
			Logger.log("Sending " + deviceType + " " + version + " content.", Logger.CMP_SERVICE, Logger.LEVEL_DEBUG);
			Command reply = getProtocolHandler().findCommand(COMMAND_OUT_UPDATE_DATA);
			reply.findParam(PARAM_DEVICE_TYPE).setData(deviceType.getBytes());
			reply.findParam(PARAM_VERSION).setData(version.getBytes());
			reply.findParam(PARAM_UPDATE_CONTENT).setData(content);
			try {
				 blockingSendTo(conn, reply);
			} catch (InterruptedException e) {
				Logger.log("Could not send update data to client: SEND_ERROR", Logger.CMP_SERVICE, Logger.LEVEL_WARNING);
			}
		}
	}

	private void announceNewUpdate(String[] deviceTypes, String[] versions) {
		if (deviceTypes.length == 0) {
			Logger.log("No updates to announce. Use \"" + COMMAND_IN_ADD_JAR + "\" command to add an update.", Logger.CMP_SERVICE, Logger.LEVEL_INFO);
			return;
		}
		Command cmd = getProtocolHandler().findCommand(COMMAND_OUT_UPDATE_DEVICE_TYPES);
		String concDeviceTypes = null;
		String concVersions = null;
		for (int i = 0; i < deviceTypes.length; ++i) {
			if (concVersions != null) {
				concDeviceTypes += PARAM_VALUE_SEPARATOR;
				concVersions += PARAM_VALUE_SEPARATOR;
			} else {
				concDeviceTypes = "";
				concVersions = "";
			}
			concDeviceTypes += deviceTypes[i];
			concVersions += versions[i];
		}
		cmd.findParam(PARAM_DEVICE_TYPE).setData(concDeviceTypes.getBytes());
		cmd.findParam(PARAM_VERSION).setData(concVersions.getBytes());
		sendToAll(cmd);
	}

	public void start() {
		setStatus(PRDService.FULLY_OPERATIONAL);
		super.start();
	}
	
	private class UpdateEntry {
		String version;
		byte[] content;
		public UpdateEntry(String version, byte[] content) {
			this.version = version;
			this.content = content;
		}
	}
}
