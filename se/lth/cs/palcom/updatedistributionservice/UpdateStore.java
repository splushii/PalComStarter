package se.lth.cs.palcom.updatedistributionservice;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import se.lth.cs.palcom.io.File;
import se.lth.cs.palcom.io.FileSystem;
import se.lth.cs.palcom.logging.Logger;

/**
 * Used to keep track of updates in the UpdateDistributionService. Reads, saves and deletes updates 
 * from the service file system folder, and gives easy access to the latest update.
 * @author Christian Hernvall
 *
 */

class UpdateStore {
	private FileSystem serviceRoot;
	private HashMap<String, HashMap<String, HashMap<String, UpdateEntry>>> updateMap;
	private HashMap<String, HashMap<String, UpdateEntry>> latestUpdateMap;
	UpdateStore(FileSystem serviceRoot) {
		this.serviceRoot = serviceRoot;
		updateMap = new HashMap<String, HashMap<String, HashMap<String, UpdateEntry>>>();
		latestUpdateMap = new HashMap<String, HashMap<String, UpdateEntry>>();
	}
	boolean parseUpdateFileSystem(Set<String> set) {
		for (String implementation: set) {
			try {
				File implementationFolder = serviceRoot.getFile(implementation);
				if (implementationFolder.isDirectory()) {
					for (File deviceTypeFolder: implementationFolder.listFiles()) {
						String deviceType = deviceTypeFolder.getName();
						if (deviceTypeFolder.isDirectory()) {
							for (File executable: deviceTypeFolder.listFiles()) {
								String version = getVersionFromFileName(executable.getName());
								String suffix = getSuffixFromFileName(executable.getName());

								UpdateEntry updateEntry = new UpdateEntry(executable, implementation, deviceType, version, suffix);
								addToMaps(updateEntry);
							}
						} else {
							Logger.log("Device type folder \"" + implementation + "\" is not a folder", Logger.CMP_SERVICE, Logger.LEVEL_WARNING);
						}
					}
				} else {
					Logger.log("Implementation folder \"" + implementation + "\" is not a folder", Logger.CMP_SERVICE, Logger.LEVEL_WARNING);
				}
			} catch (IOException e) {
				Logger.log("Can not find/access updates for the implementation: " + implementation, Logger.CMP_SERVICE, Logger.LEVEL_DEBUG);
				continue;
			}
		}
		return true;
	}
		
	private String getSuffixFromFileName(String name) {
		String[] end = name.split("-")[1].split("\\.");
		return end[end.length - 1];
	}
	
	private String getVersionFromFileName(String name) {
		String[] end = name.split("-")[1].split("\\.");
		return end[0] + "." + end[1] + "." + end[2];
	}
	
	boolean saveUpdate(String implementation, String deviceType, String version, String suffix, byte[] content) {
		File f;
		try {
			f = serviceRoot.getFile(implementation + "/" + deviceType + "/" + deviceType + "-" + version + suffix, true);
		} catch (IOException e1) {
			Logger.log("Could not access/create update file.", Logger.CMP_SERVICE, Logger.LEVEL_WARNING);
			return false;
		}
		try {
			OutputStream os = f.getOutputStream();
			os.write(content);
			os.flush();
			os.close();
		} catch (IOException e) {
			Logger.log("Could not write update content to file.", Logger.CMP_SERVICE, Logger.LEVEL_WARNING);
			return false;
		}
		UpdateEntry updateEntry = new UpdateEntry(f, implementation, deviceType, version, suffix);
		addToMaps(updateEntry);
		return true;
	}
	
	private void addToMaps(UpdateEntry updateEntry) {
		HashMap<String, HashMap<String, UpdateEntry>> deviceTypeMap = updateMap.get(updateEntry.implementation);
		if (deviceTypeMap == null) {
			deviceTypeMap = new HashMap<String, HashMap<String, UpdateEntry>>();
			updateMap.put(updateEntry.implementation, deviceTypeMap);
		}
		HashMap<String, UpdateEntry> versionMap = deviceTypeMap.get(updateEntry.deviceType);
		if (versionMap == null) {
			versionMap = new HashMap<String, UpdateEntry>();
			deviceTypeMap.put(updateEntry.deviceType, versionMap);
		}
		versionMap.put(updateEntry.version, updateEntry);
		
		HashMap<String, UpdateEntry> latestDeviceTypeMap = latestUpdateMap.get(updateEntry.implementation);
		if (latestDeviceTypeMap == null) {
			latestDeviceTypeMap = new HashMap<String, UpdateEntry>();
			latestUpdateMap.put(updateEntry.implementation, latestDeviceTypeMap);
		}
		UpdateEntry currentLatestUpdateEntry = latestDeviceTypeMap.get(updateEntry.deviceType);
		if (currentLatestUpdateEntry == null || updateEntry.isVersionGreaterThan(currentLatestUpdateEntry)) {
			latestDeviceTypeMap.put(updateEntry.deviceType, updateEntry);
			Logger.log("Latest update for device type " + updateEntry.deviceType + " is " + updateEntry.version, Logger.CMP_SERVICE, Logger.LEVEL_INFO);
		}
	}
	
	boolean deleteUpdate(String implementation, String deviceType, String version) {
		HashMap<String, UpdateEntry> versionMap;
		if (updateMap.containsKey(implementation)) {
			HashMap<String, HashMap<String, UpdateEntry>> deviceTypeMap = updateMap.get(implementation);
			if (deviceTypeMap.containsKey(deviceType)) {
				versionMap = deviceTypeMap.get(deviceType);
				if (versionMap.containsKey(version)) {
					UpdateEntry updateEntry = versionMap.remove(version);
					if (!updateEntry.executableFile.delete()) {
						versionMap.put(version, updateEntry);
						Logger.log("Could not delete update: " + updateEntry.executableFile.getName(), Logger.CMP_SERVICE, Logger.LEVEL_WARNING);
						return false;
					}
				} else {
					return false;
				}
			} else {
				return false;
			}
		} else {
			return false;
		}
		
		if (latestUpdateMap.containsKey(implementation)) {
			HashMap<String, UpdateEntry> latestDeviceTypeMap = latestUpdateMap.get(implementation);
			if (latestDeviceTypeMap.get(deviceType).version.equals(version)) {
				latestDeviceTypeMap.remove(deviceType);
				String latestVersion = null;
				for (String v: versionMap.keySet()) {
					if (UpdateEntry.compareVersion(v, latestVersion) > 0) {
						latestVersion = v;
					}
				}
				if (latestVersion != null) {
					latestDeviceTypeMap.put(deviceType, versionMap.get(latestVersion));									
				}
			}
		}
		return false;
	}

	void deleteAllOldUpdates() {
		for (String implementation: updateMap.keySet()) {
			HashMap<String, HashMap<String, UpdateEntry>> deviceTypeMap = updateMap.get(implementation);
			for (String deviceType: deviceTypeMap.keySet()) {
				String latestVersion = latestUpdateMap.get(implementation).get(deviceType).version;
				HashMap<String, UpdateEntry> versionMap = deviceTypeMap.get(deviceType);
				for (String version: versionMap.keySet()) {
					if (UpdateEntry.compareVersion(version, latestVersion) != 0) {
						UpdateEntry updateEntry = versionMap.remove(version);
						updateEntry.executableFile.delete();
					}
				}
			}
		}
	}
	
	void deleteAllUpdates() {
		serviceRoot.delete();
		updateMap = new HashMap<String, HashMap<String, HashMap<String, UpdateEntry>>>();
		latestUpdateMap = new HashMap<String, HashMap<String, UpdateEntry>>();
	}
	
	UpdateEntry getLatestUpdate(String implementation, String deviceType) {
		HashMap<String, UpdateEntry> latestDeviceTypeMap = latestUpdateMap.get(implementation);
		if (latestDeviceTypeMap == null)
			return null;
		return latestDeviceTypeMap.get(deviceType);
	}
	
	UpdateEntry getUpdate(String implementation, String deviceType, String version) {
		// Slow but concise
		if (updateMap.containsKey(implementation)) {
			if (updateMap.get(implementation).containsKey(deviceType)) {
				if (updateMap.get(implementation).get(deviceType).containsKey(version)) {
					return updateMap.get(implementation).get(deviceType).get(version);
				}
			}
		}
		return null;
	}
	
	Set<UpdateEntry> getLatestUpdates() {
		Set<UpdateEntry> ret = new HashSet<UpdateEntry>();
		for (String implementation: latestUpdateMap.keySet()) {
			HashMap<String, UpdateEntry> latestDeviceTypeMap = latestUpdateMap.get(implementation);
			if (latestDeviceTypeMap != null) {
				ret.addAll(latestDeviceTypeMap.values());
			}
		}
		return ret;
	}
	
	String getLatestUpdatesInText() {
		StringBuilder sb = new StringBuilder();
		for (String implementation: latestUpdateMap.keySet()) {
			HashMap<String, UpdateEntry> latestDeviceTypeMap = latestUpdateMap.get(implementation);
			for (String deviceType: latestDeviceTypeMap.keySet()) {
				sb.append("[" + implementation + "] " + deviceType + " " + latestDeviceTypeMap.get(deviceType).version + "\n");
			}
		}
		return sb.toString();
	}
	
	String getAllUpdatesInText() {
		StringBuilder sb = new StringBuilder();
		for (String implementation: updateMap.keySet()) {
			HashMap<String, HashMap<String, UpdateEntry>> deviceTypeMap = updateMap.get(implementation);
			for (String deviceType: deviceTypeMap.keySet()) {
				HashMap<String, UpdateEntry> versionMap = deviceTypeMap.get(deviceType);
				sb.append("[" + implementation + "] " + deviceType);					
				for (String version: versionMap.keySet()) {
					sb.append(" " + version);
				}
				sb.append("\n");
			}
		}
		return sb.toString();
	}
}
