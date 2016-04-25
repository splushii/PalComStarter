package se.lth.cs.palcom.updatedistributionservice;

import se.lth.cs.palcom.io.File;

/**
 * Data structure used to keep track of update attributes.
 * @author Christian Hernvall
 *
 */
class UpdateEntry {

	File executableFile;
	String implementation;
	String deviceType;
	String version;
	String suffix;

	UpdateEntry(File executable, String implementation, String deviceType, String version, String suffix) {		
		this.executableFile = executable;
		this.implementation = implementation;
		this.deviceType = deviceType;
		this.version = version;
		this.suffix = suffix;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((deviceType == null) ? 0 : deviceType.hashCode());
		result = prime * result + ((implementation == null) ? 0 : implementation.hashCode());
		result = prime * result + ((version == null) ? 0 : version.hashCode());
		return result;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		UpdateEntry other = (UpdateEntry) obj;
		if (deviceType == null) {
			if (other.deviceType != null)
				return false;
		} else if (!deviceType.equals(other.deviceType))
			return false;
		if (implementation == null) {
			if (other.implementation != null)
				return false;
		} else if (!implementation.equals(other.implementation))
			return false;
		if (version == null) {
			if (other.version != null)
				return false;
		} else if (!version.equals(other.version))
			return false;
		return true;
	}
	
	int compareVersionWith(UpdateEntry otherEntry) {
		return compareVersion(version, otherEntry.version);
	}
	
	static int compareVersion(String version1, String version2) {
		if (version1 == null)
			return -1;
		if (version2 == null)
			return 1;
		String[] split1 = version1.split("\\.");
		String[] split2 = version2.split("\\.");
		for (int i = 0; i < (split1.length < split2.length ? split1.length : split2.length); i++) {
			Integer n1 = Integer.parseInt(split1[i]);
			Integer n2 = Integer.parseInt(split2[i]);
			if (n1 > n2) {
				return 1;
			} else if (n2 > n1) {
				return -1;
			}
		}
		return 0;
	}

	public boolean isVersionGreaterThan(UpdateEntry otherEntry) {
		return compareVersionWith(otherEntry) > 0 ? true : false;
	}
}