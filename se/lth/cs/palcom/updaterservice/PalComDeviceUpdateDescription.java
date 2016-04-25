package se.lth.cs.palcom.updaterservice;

/**
 * Data structure used by UpdaterService to keep track of PalCom Device update attributes.
 * @author splushii
 *
 */
class PalComDeviceUpdateDescription {
	static final int INCOMPATIBLE_VERSION_SCHEME = -1;
	static final int MAJOR = 0;
	static final int MINOR = 1;
	static final int PATCH = 2;
	static final int IDENTICAL_VERSION = 3;

	String deviceType;
	String pathToExec;
	String currentVersion;
	String newVersion;
	int currentMajor;
	int currentMinor;
	int currentPatch;
	int newMajor;
	int newMinor;
	int newPatch;
	int updateType;
	boolean upgrade;
	
	PalComDeviceUpdateDescription(String deviceType, String currentVersion, String newVersion) {
		this.currentVersion = currentVersion;
		this.newVersion = newVersion;
		this.deviceType = deviceType;
		String[] splitCurrentVersion = currentVersion.split("\\.");
		String[] splitNewVersion = newVersion.split("\\.");
		if (splitCurrentVersion.length != splitNewVersion.length || splitCurrentVersion.length != 3) {
			updateType = INCOMPATIBLE_VERSION_SCHEME;
			upgrade = false;
			return;			
		}
		int currentMajor = Integer.parseInt(splitCurrentVersion[0]);
		int newMajor = Integer.parseInt(splitNewVersion[0]);
		if (newMajor > currentMajor) {
			updateType = MAJOR;
			upgrade = true;
			return;
		} else if (newMajor < currentMajor) {
			updateType = MAJOR;
			upgrade = false;
			return;
		}
		int currentMinor = Integer.parseInt(splitCurrentVersion[1]);
		int newMinor = Integer.parseInt(splitNewVersion[1]);
		if (newMinor > currentMinor) {
			updateType = MINOR;
			upgrade = true;
			return;
		} else if (newMinor < currentMinor) {
			updateType =  MINOR;
			upgrade = false;
			return;
		}
		int currentPatch = Integer.parseInt(splitCurrentVersion[2]);
		int newPatch = Integer.parseInt(splitNewVersion[2]);
		if (newPatch > currentPatch) {
			updateType =  PATCH;
			upgrade = true;
			return;
		} else if (newPatch < currentPatch) {
			updateType = PATCH;
			upgrade = false;
			return;
		}
		updateType = IDENTICAL_VERSION;
		upgrade = false;
	}
	boolean isProtocolBreaking() {
		return updateType == MAJOR ? true : false;
	}
	boolean isIdentical() {
		return updateType == IDENTICAL_VERSION ? true : false;
	}
	boolean isIncompatible() {
		return updateType == INCOMPATIBLE_VERSION_SCHEME ? true : false;
	}
	boolean isUpgrade() {
		return upgrade;
	}
}
