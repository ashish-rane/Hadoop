package com.windows.fs;

import java.io.IOException;

import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

public class WindowsLocalFileSystem extends LocalFileSystem{

	/**
	 *
	 *
	 */
	public WindowsLocalFileSystem() {
		
		super();
	}
	 
	 
	public boolean mkdirs(final Path path, final FsPermission permission)
			throws IOException {
		
		final boolean result = super.mkdirs(path);
		this.setPermission(path, permission);
		return result;
	}

	public void setPermission(final Path path, final FsPermission permission)
			throws IOException {
		
		try {
			super.setPermission(path, permission);
		} catch (final IOException e) {
			System.err
					.println("Cant help it, hence ignoring IOException setting persmission for path \""
							+ path + "\": " + e.getMessage());
		}
	}

}
