package se.lth.cs.palcom.palcomstarter;

import java.io.File;
import java.io.IOException;

import ist.palcom.resource.descriptor.DeviceID;
import se.lth.cs.palcom.device.AbstractDevice;
import se.lth.cs.palcom.filesystem.HostFileSystems;
import se.lth.cs.palcom.service.FSServiceManager;
import se.lth.cs.palcom.updaterservice.UpdaterService;
import se.lth.cs.palcom.util.configuration.DeviceList;
import se.lth.cs.palcom.util.configuration.DeviceList.DeviceItem;
import se.lth.cs.palcom.logging.Logger;

/** 
 * PalCom device that acts as a monitoring device. Uses functionality in {@link UpdaterService}
 * @author Christian Hernvall
 *
 */
public class PalComStarter extends AbstractDevice {
	private static final String VERSION = "1.0.0";
	private static final String COM_HELP = "h";
	private final static String COM_DEVICE = "x";
	private static final String COM_FILE_SYSTEM = "f";
	
	public final static String DEVICE_TYPE = "PalComStarter";
	public static final String COM_CONTINUE_UPDATE_STAGE_THREE = "-continue-update-stage-three";
	
	private static final int DEFAULT_LOG_LEVEL = Logger.LEVEL_INFO;

	public PalComStarter(DeviceItem deviceItem, String version, boolean continueUpdateStageThree) {
		super(deviceItem.getID(), deviceItem.getName(), version);
		Logger.setLevel(DEFAULT_LOG_LEVEL);
		FSServiceManager sm = new FSServiceManager(this);
		sm.enableManagerService();
		
		UpdaterService us = new UpdaterService(this, continueUpdateStageThree);
		us.register();
		us.start();
	}
	
	public static void main(String[] args) {
		DeviceID deviceID = null;
		String palcomFSPath = null;
		boolean continueUpdateStageThree = false;
		
		System.out.println("Arguments:");
		for (String arg: args) {
			System.out.println(arg);
		}
		System.out.println();
		for (int i = 0; i < args.length; ++i) {
			if (args[i].equals("-x")) {
				if (args.length > i + 1) {
					deviceID = new DeviceID(args[i + 1]);					
				}
			} else if (args[i].equals("-"+COM_FILE_SYSTEM)) {
				// Check path etc ...
				if (args.length > i + 1) {
					palcomFSPath = args[i + 1];
					try{
						File f = new File(palcomFSPath);
						if(f.isAbsolute()&&f.isDirectory()){
							HostFileSystems.setDeviceRootPath(palcomFSPath);
						} else {
							Logger.log("Incorrect usage, -" + COM_FILE_SYSTEM + " " +palcomFSPath
									+ " must be a valid path to an existing directory", Logger.CMP_DEVICE, Logger.LEVEL_ERROR);
							printUsage();
							Logger.log("Exiting", Logger.CMP_DEVICE, Logger.LEVEL_ERROR);
							System.exit(1);
						}
					}catch(Exception ex){
						Logger.log("Incorrect usage, -" + COM_FILE_SYSTEM + " " +palcomFSPath
								+ " must be a valid path to an existing directory", Logger.CMP_DEVICE, Logger.LEVEL_ERROR);
						printUsage();
						Logger.log("Exiting", Logger.CMP_DEVICE, Logger.LEVEL_ERROR);
						System.exit(1);
					}
					Logger.log(" -f using path: " + palcomFSPath, Logger.CMP_DEVICE, Logger.LEVEL_DEBUG);									
				} else{
					Logger.log("-" + COM_FILE_SYSTEM + " used without specifying a path.", Logger.CMP_DEVICE, Logger.LEVEL_ERROR);
					printUsage();
					Logger.log("Exiting", Logger.CMP_DEVICE, Logger.LEVEL_ERROR);
					System.exit(1);
				}
			} else if (args[i].equals("-" + COM_CONTINUE_UPDATE_STAGE_THREE)) {
				continueUpdateStageThree = true;
				Logger.log("Will continue in stage three.", Logger.CMP_DEVICE, Logger.LEVEL_DEBUG);
			} else if (args[i].equals("-" + COM_HELP)) {
				printUsage();
				System.exit(0);
			}
		}
		
		DeviceItem[] configurations = null;
		DeviceItem deviceItem = null;
		if (deviceID == null) {
			Logger.log("Device ID not set. Choosing the first default device configuration. Generates a new configuration if none is present.", Logger.CMP_DEVICE, Logger.LEVEL_INFO);
			try {
				configurations = DeviceList.getConfigurations(DEVICE_TYPE, true);
			} catch (IOException ex) {
				Logger.log("Failed to access default device ID. Cannot start device.", Logger.CMP_DEVICE, Logger.LEVEL_ERROR);
				Logger.log(ex.getMessage(), Logger.CMP_DEVICE, Logger.LEVEL_ERROR);
				System.exit(1);
			}
			deviceItem = configurations[0];
		} else {
			try {
				configurations = DeviceList.getConfigurations(DEVICE_TYPE, false);
			} catch (IOException e) {
				Logger.log("Failed to access default device ID. Cannot start device.", Logger.CMP_DEVICE, Logger.LEVEL_ERROR);
				System.exit(1);
			}
			deviceItem = getDeviceItem(configurations, deviceID.getID());
			if (deviceItem == null) {
				Logger.log("Unable to find the configuration with id: "
						+ deviceID.getID(), Logger.CMP_DEVICE, Logger.LEVEL_ERROR);
				Logger.log("Exiting", Logger.CMP_DEVICE, Logger.LEVEL_ERROR);
				System.exit(1);
			}
		}
		
		if (palcomFSPath == null) {
			Logger.log("PalCom Filesystem path not set. Will use default path: " + HostFileSystems.getUnixStylePathToFilesystemRoot(), Logger.CMP_DEVICE, Logger.LEVEL_INFO);
		}
		
		new PalComStarter(deviceItem, VERSION, continueUpdateStageThree).start();
	}
	
	private static void printUsage() {
		System.out.println(" --- PalComStarter options ---");
		System.out
				.println("  -"+COM_HELP+"                                      --- Print this help");
		System.out
				.println("  -"+COM_DEVICE+" <device id>                          --- Use PalComStarter with device id <device id>.");
		System.out
				.println("  -"+COM_FILE_SYSTEM+" <dir-path>                           --- Start PalComStarter with PalcomFilesystem located in existing directory at <dir-path>");
		System.out
				.println("  -"+COM_CONTINUE_UPDATE_STAGE_THREE+"                      --- Start PalComStarter in update stage three in order to continue an ongoing updating process. This flag should only be set manually during testing. DO NOT USE UNLESS YOU KNOW WHAT IT DOES!");

	}
	
	private static DeviceItem getDeviceItem(DeviceItem[] configurations,
			String deviceId) {
		for (int i = 0; i < configurations.length; i++) {
			if (configurations[i].getID().getID().equals(deviceId)) {
				return configurations[i];
			}
		}
		return null;
	}
}
