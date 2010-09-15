/*
This file is part of leafdigital IpToCountry.

IpToCountry is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

IpToCountry is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with IpToCountry.  If not, see <http://www.gnu.org/licenses/>.

Copyright 2010 Samuel Marshall.
*/
package com.leafdigital.iptocountry;

import java.io.*;
import java.net.*;
import java.util.HashMap;
import java.util.regex.*;
import java.util.zip.GZIPInputStream;

/**
 * Converts IP addresses to country codes using the database available from
 * http://software77.net/geo-ip/
 * <p>
 * This class loads the database into memory, where it is stored in an
 * efficient format for instant lookups. It is capable of automatically
 * updating the database on a given time schedule.
 * <p>
 * A disk folder is required to store the latest database version so that
 * it can be loaded initially without requesting it over HTTP. This is
 * important because the list maintainers restrict the number of requests
 * permitted.
 * <p>
 * At present, this class only supports the IP4 database.
 */
public class IpToCountry
{
	/**
	 * Default location of database. This address points to the .gz compressed
	 * version of the IP4 database.
	 */
	public final static String DOWNLOAD_LOCATION =
		"http://software77.net/geo-ip/?DL=1";

	/**
	 * Expire constant (milliseconds): The database will be retrieved each day.
	 */
	public final static long EXPIRE_DAILY = 24 * 60 * 60 * 1000L;

	/**
	 * Expire constant (milliseconds): The database will be retrieved each week.
	 */
	public final static long EXPIRE_WEEKLY = 7 * EXPIRE_DAILY;

	/**
	 * Expire constant (milliseconds): The database will be retrieved every
	 * 30 days. Recommended.
	 */
	public final static long EXPIRE_30DAYS = 30 * EXPIRE_DAILY;

	private final static long DOWNLOAD_LIMIT = 8 * 60 * 60 * 1000L;
	private final static long ATTEMPT_LIMIT = 60 * 1000L;
	private final static String FILE_NAME = "IpToCountry.csv";
	private final static Pattern CSV_LINE = Pattern.compile(
		"^\"([0-9]{1,10})\",\"([0-9]{1,10})\",\"[^\"]*\",\"[0-9]*\",\"([A-Z]+)\"");
	private final static int COPY_BUFFER = 65536;
	private final static int INITIAL_ENTRIES = 65536;

	private File folder;
	private URL downloadLocation;
	private long expireTime;

	private long fileLastModified;

	private long lastDownloadTime, lastAttemptTime;
	private DownloadThread thread;
	private Informer informer;

	long[] entryFrom, entryTo;
	String[] entryCode;
	Throwable lastError;

	/**
	 * Interface for use to receive information about IpToCountry processing,
	 * such as errors when downloading the file.
	 */
	public interface Informer
	{
		/**
		 * Called if there is an error downloading the file.
		 * @param t Exception
		 */
		public void downloadFailed(Throwable t);

		/**
		 * Called if the file downloads successfully, but cannot be loaded.
		 * @param t Exception
		 */
		public void loadFailed(Throwable t);

		/**
		 * Called if a line in the file is not understood.
		 * @param errorIndex 0 for first error line, etc
		 * @param line Text of line
		 */
		public void lineError(int errorIndex, String line);

		/**
		 * Called once a file has been downloaded.
		 * @param milliseconds Time it took to download the file.
		 * @param bytes Size of file
		 */
		public void fileDownloaded(int milliseconds, int bytes);

		/**
		 * Called once a file has been successfully loaded.
		 * @param entries Number of entries processed
		 * @param milliseconds Time it took to load the file (does not include
		 *   download)
		 * @param lineErrors Number of lines which caused errors
		 */
		public void fileLoaded(int entries, int milliseconds, int lineErrors);
	}

	/**
	 * Default informer that writes errors to standard error.
	 */
	private class DefaultInformer implements Informer
	{
		@Override
		public void downloadFailed(Throwable t)
		{
			System.err.println("IpToCountry: An error occurred when downloading from "
				+ downloadLocation + ":");
			t.printStackTrace();
			System.err.println();
		}

		@Override
		public void loadFailed(Throwable t)
		{
			System.err.println("IpToCountry: An error occurred when loading the new "
				+ "data file " + getDownloadFile() + ":");
			t.printStackTrace();
			System.err.println();
		}

		@Override
		public void lineError(int errorIndex, String line)
		{
			if(errorIndex == 0)
			{
				System.err.println("IpToCountry: File line not understood (displaying "
					+ "first occurrence only):");
				System.err.println(line);
				System.err.println();
			}
		}

		@Override
		public void fileDownloaded(int milliseconds, int bytes)
		{
			System.err.println("IpToCountry: Downloaded new data file (" + bytes
				+ " bytes) in " + milliseconds + " ms");
		}

		@Override
		public void fileLoaded(int entries, int milliseconds, int lineErrors)
		{
			System.err.println("IpToCountry: Loaded data file (" + entries
				+ " entries) in " + milliseconds + " ms; " + lineErrors
				+ " lines were not understood");
		}
	}

	/**
	 * Thread used to download new file.
	 */
	private class DownloadThread extends Thread
	{
		private DownloadThread()
		{
			start();
		}

		@Override
		public void run()
		{
			try
			{
				File downloadFile = getDownloadFile();

				// Download file
				try
				{
					downloadFile();
				}
				catch(Throwable t)
				{
					synchronized(IpToCountry.this)
					{
						lastError = t;
					}
					informer.downloadFailed(t);
					return;
				}

				// Load file
				try
				{
					loadFile(downloadFile);
					renameDownloadedToReal();
				}
				catch(Throwable t)
				{
					synchronized(IpToCountry.this)
					{
						lastError = t;
					}
					informer.loadFailed(t);
				}
			}
			finally
			{
				synchronized(IpToCountry.this)
				{
					thread = null;
					IpToCountry.this.notifyAll();
				}
			}
		}
	}

	/**
	 * Initialises the database. The database will be loaded from a stored
	 * file in the given folder. If there is no database present, or it is
	 * out of date, a new one will be downloaded and obtained.
	 * <p>
	 * This constructor returns immediately after loading the file from disk
	 * (if present); any download will happen on another thread. If there is
	 * no file on disk, the constructor will block until the file has been
	 * downloaded and fully loaded (or there is a network timeout or other
	 * problem).
	 * @param folder Folder for database storage (must be writable)
	 * @param downloadLocation URL of database for download
	 * @param expireTime Time in milliseconds before database expires and a
	 *   new one is downloaded (use EXPIRE_xx constant)
	 * @param informer Handler that will be informed if there is a download
	 *   problem during the lifetime of this object (null = no handler, write
	 *   errors to standard error)
	 * @throws IllegalArgumentException If the folder is not writable, or
	 *   expire time is less than a day
	 * @throws MalformedURLException If the download URL is not valid
	 * @throws IOException If there is any problem loading the file
	 * @throws InterruptedException If the constructor attempts to block,
	 *   but is interrupted
	 */
	public IpToCountry(File folder, String downloadLocation, long expireTime,
		Informer informer)
		throws IllegalArgumentException, MalformedURLException, IOException,
			InterruptedException
	{
		this.folder = folder;
		this.expireTime = expireTime;
		this.downloadLocation = new URL(downloadLocation);
		if(informer != null)
		{
			this.informer = informer;
		}
		else
		{
			this.informer = new DefaultInformer();
		}

		if(expireTime < EXPIRE_DAILY)
		{
			throw new IllegalArgumentException("Cannot set expire time lower than "
				+ "one day");
		}
		if(!folder.canWrite())
		{
			throw new IllegalArgumentException("Folder '" + folder + "' is not "
				+ "writable; check permissions");
		}

		File file = getFile();
		boolean download = false;
		if(file.exists())
		{
			loadFile(file);
			download = expired();
		}
		else
		{
			download = true;
		}

		// Guarantee that when this completes, the 'entry' arrays will be set
		synchronized(this)
		{
			if(download)
			{
				startDownload();
				if(entryFrom == null)
				{
					if(thread == null)
					{
						// This should not actually happen
						throw new IOException("Download prevented");
					}
					wait();
					if(entryFrom == null)
					{
						if(lastError != null)
						{
							if(lastError instanceof IOException)
							{
								throw (IOException)lastError;
							}
							else
							{
								IOException e = new IOException("Error obtaining database");
								e.initCause(lastError);
								throw e;
							}
						}
						throw new IOException("Unknown error obtaining database");
					}
				}
			}
		}
	}

	/**
	 * Initialises the database. This is a convenience constructor;
	 * the default download location and expiry period (30 days) will be used.
	 * @param folder Folder for database storage (must be writable)
	 * @param informer Handler that will be informed if there is a download
	 *   problem during the lifetime of this object (null = no handler, write
	 *   errors to standard error)
	 * @throws IllegalArgumentException If the folder is not writable
	 * @throws IOException If there is any problem loading the file
	 * @throws InterruptedException If the constructor attempts to block,
	 *   but is interrupted
	 */
	public IpToCountry(File folder, Informer informer)
		throws IllegalArgumentException, IOException,	InterruptedException
	{
		this(folder, DOWNLOAD_LOCATION, EXPIRE_30DAYS, informer);
	}

	/**
	 * Gets the country code for an IP address.
	 * <p>
	 * Based on the list documentation, this is the ISO 3166 2-letter code of the
	 * organisation to which the allocation or assignment was made, with the
	 * following differences or unusual cases:
   * <ul>
   * <li>AP - non-specific Asia-Pacific location</li>
   * <li>CS - Serbia and Montenegro</li>
   * <li>YU - Serbia and Montenegro (Formally Yugoslavia) (Being phased out)</li>
   * <li>EU - non-specific European Union location</li>
   * <li>FX - France, Metropolitan</li>
   * <li>PS - Palestinian Territory, Occupied</li>
   * <li>UK - United Kingdom (standard says GB)</li>
   * <li>ZZ - IETF RESERVED address space.</li>
   * <li>.. - Unmatched address (specific to this system, not in list) </li>
   * </ul>
	 * @param address Internet address
	 * @return Country code
	 * @throws IllegalArgumentException If the address is an IPv6 address that
	 *   can't be expressed in 4 bytes, or something else
	 */
	public String getCountryCode(InetAddress address)
		throws IllegalArgumentException
	{
		long addressLong = getAddressAsLong(get4ByteAddress(address));
		synchronized(this)
		{
			long now = getCurrentTime();
			if(now > fileLastModified + expireTime)
			{
				startDownload();
			}
			return getCountryCode(addressLong, entryFrom, entryTo, entryCode);
		}
	}

	/**
	 * Get a country code given the arrays which hold the details. (This is
	 * a static method to make unit testing easier.)
	 * <p>
	 * This is a separate method with default access so it can be used in unit
	 * testing.
	 * @param addressLong Address as long
	 * @param entryFrom Array of 'from' values for each entry
	 * @param entryTo Array of 'to' values for each entry
	 * @param entryCode Array of country codes for each entry'
	 * @return Corresponding country code, or .. if not known
	 */
	static String getCountryCode(long addressLong,
		long[] entryFrom, long[] entryTo, String[] entryCode)
	{
		// Binary search for the highest entryFrom that's less than or equal to
		// the specified address.
		int min = 0, max = entryFrom.length == 0 ? 0 : entryFrom.length - 1;
		while(min != max)
		{
			int half = (min + max) / 2;
			if(half == min)
			{
				// This special case handles the situation where e.g. min=10, max=11;
				// there half=10 and we could get an endless loop.
				if(entryFrom[max] <= addressLong)
				{
					min = max;
				}
				else
				{
					max = min;
				}
			}
			else
			{
				// Standard case; check whether the halfway position is bigger or
				// smaller
				if(entryFrom[half] <= addressLong)
				{
					min = half;
				}
				else
				{
					max = half - 1;
				}
			}
		}

		if(min >= entryFrom.length || entryTo[min] < addressLong
			|| entryFrom[min] > addressLong)
		{
			return "..";
		}
		else
		{
			return entryCode[min];
		}
	}

	/**
	 * Given an internet address in network byte order, converts it into an
	 * unsigned long.
	 * <p>
	 * This is a separate method with default access so it can be used in unit
	 * testing.
	 * @param bytes Bytes
	 * @return Long value
	 * @throws IllegalArgumentException If there aren't 4 bytes of address
	 */
	static long getAddressAsLong(byte[] bytes)
		throws IllegalArgumentException
	{
		if(bytes.length != 4)
		{
			throw new IllegalArgumentException("Input must be 4 bytes");
		}
		int
			i0 = (int)bytes[0] & 0xff,
			i1 = (int)bytes[1] & 0xff,
			i2 = (int)bytes[2] & 0xff,
			i3 = (int)bytes[3] & 0xff;
		return
			(((long)i0) << 24) |
			(((long)i1) << 16) |
			(((long)i2) << 8) |
			((long)i3);
	}

	/**
	 * Converts an {@link InetAddress} into a 4-byte address.
	 * <p>
	 * This is a separate method with default access so it can be used in unit
	 * testing.
	 * @param address Address
	 * @return 4 bytes in network byte order
	 * @throws IllegalArgumentException If the address is an IPv6 address that
	 *   can't be expressed in 4 bytes, or something else
	 */
	static byte[] get4ByteAddress(InetAddress address)
		throws IllegalArgumentException
	{
		byte[] actual = address.getAddress();
		if(actual.length == 4)
		{
			return actual;
		}

		if(address instanceof Inet6Address)
		{
			if(((Inet6Address)address).isIPv4CompatibleAddress())
			{
				// For compatible addresses, use last 4 bytes
				byte[] bytes = new byte[4];
				System.arraycopy(actual, actual.length - 4, bytes, 0, 4);
				return bytes;
			}
			else
			{
				throw new IllegalArgumentException("IPv6 addresses not supported "
					+ "unless IP4 compatible): " + address.getHostAddress());
			}
		}

		throw new IllegalArgumentException("Unknown address type: "
			+ address.getHostAddress());
	}

	/**
	 * Gets the main csv file.
	 * <p>
	 * This is a separate method with default access so it can be used in unit
	 * testing.
	 * @return File object pointing to cached file
	 */
	File getFile()
	{
		return new File(folder, FILE_NAME);
	}

	/**
	 * Gets the csv file used while downloading.
	 * <p>
	 * This is a separate method with default access so it can be used in unit
	 * testing.
	 * @return File object pointing to new file for use during download
	 */
	File getDownloadFile()
	{
		return new File(folder, FILE_NAME + ".download");
	}

	/**
	 * Gets the csv file used temporarily while renaming in the new one.
	 * <p>
	 * This is a separate method with default access so it can be used in unit
	 * testing.
	 * @return File object pointing to old cached file (temporary only)
	 */
	File getOldFile()
	{
		return new File(folder, FILE_NAME + ".old");
	}

	/**
	 * @return True if the currently-loaded file has expired
	 */
	private boolean expired()
	{
		return fileLastModified + expireTime < getCurrentTime();
	}

	/**
	 * Loads the existing file from disk into memory.
	 * <p>
	 * This is a separate method with default access so it can be used in unit
	 * testing.
	 * @param file File to load
	 * @throws IOException Any problem loading file
	 */
	void loadFile(File file) throws IOException
	{
		BufferedReader reader = null;
		try
		{
			// String-sharing buffer
			HashMap<String, String> countryCodes = new HashMap<String, String>();

			// Get file time
			long newFileLastModified = file.lastModified();

			// Get start time
			long start = getCurrentTime();

			// Prepare new index buffers
			long[] newEntryFrom = new long[INITIAL_ENTRIES];
			long[] newEntryTo = new long[INITIAL_ENTRIES];
			String[] newEntryCode = new String[INITIAL_ENTRIES];
			int entries = 0;

			reader = new BufferedReader(
				new InputStreamReader(new FileInputStream(file), "US-ASCII"));
			int lineErrors = 0;
			while(true)
			{
				// Read next line
				String line = reader.readLine();
				if(line == null)
				{
					// No more lines
					break;
				}
				// Skip comments (lines beginning # or whitespace) and empty lines
				if(line.isEmpty() || line.startsWith("#")
					|| Character.isWhitespace(line.charAt(0)))
				{
					continue;
				}

				// Match line
				Matcher m = CSV_LINE.matcher(line);
				if(!m.find())
				{
					// Non-matching line; report as warning, the first time
					informer.lineError(lineErrors++, line);
					continue;
				}

				// Line matches!

				// Expand entry arrays if required
				if(entries == newEntryFrom.length)
				{
					long[] temp = new long[entries * 2];
					System.arraycopy(newEntryFrom, 0, temp, 0, entries);
					newEntryFrom = temp;
					temp = new long[entries * 2];
					System.arraycopy(newEntryTo, 0, temp, 0, entries);
					newEntryTo = temp;
					String[] tempStr = new String[entries * 2];
					System.arraycopy(newEntryCode, 0, tempStr, 0, entries);
					newEntryCode = tempStr;
				}

				// Share country strings, as they are likely to be repeated many times
				String code = m.group(3);
				if(countryCodes.containsKey(code))
				{
					code = countryCodes.get(code);
				}
				else
				{
					countryCodes.put(code, code);
				}

				// Store new entry
				newEntryFrom[entries] = Long.parseLong(m.group(1));
				newEntryTo[entries] = Long.parseLong(m.group(2));
				newEntryCode[entries] = code;
				entries++;
			}

			// Reallocate arrays to precise length
			long[] temp = new long[entries];
			System.arraycopy(newEntryFrom, 0, temp, 0, entries);
			newEntryFrom = temp;
			temp = new long[entries];
			System.arraycopy(newEntryTo, 0, temp, 0, entries);
			newEntryTo = temp;
			String[] tempStr = new String[entries];
			System.arraycopy(newEntryCode, 0, tempStr, 0, entries);
			newEntryCode = tempStr;

			// Switch arrays with 'real' ones
			synchronized(this)
			{
				entryFrom = newEntryFrom;
				entryTo = newEntryTo;
				entryCode = newEntryCode;
				fileLastModified = newFileLastModified;
			}

			// Tell informer
			informer.fileLoaded(entries, (int)(getCurrentTime() - start),
				lineErrors);
		}
		finally
		{
			if(reader != null)
			{
				reader.close();
			}
		}
	}

	/**
	 * Starts the download thread unless a thread is already running, in which
	 * case does nothing.
	 */
	private synchronized void startDownload()
	{
		// If already downloading, ignore
		if(thread != null || lastAttemptTime + ATTEMPT_LIMIT > getCurrentTime())
		{
			return;
		}
		thread = new DownloadThread();
	}

	/**
	 * Obtains current time.
	 * <p>
	 * This is a separate method with default access so it can be used in unit
	 * testing.
	 * @return Current time in milliseconds
	 */
	long getCurrentTime()
	{
		return System.currentTimeMillis();
	}

	/**
	 * Downloads the file. There are safety checks to ensure that the system
	 * does not try to download it too frequently.
	 * <p>
	 * This is a separate method with default access so it can be used in unit
	 * testing.
	 * @throws IOException Any error downloading the file, or it's less than
	 *   8 hours since the last download time.
	 */
	void downloadFile() throws IOException
	{
		File downloadFile = getDownloadFile();
		GZIPInputStream in = null;
		FileOutputStream out = null;
		long start = getCurrentTime();
		boolean ok = false;

		try
		{
			// Use this safety check to ensure that we don't download more than
			// once in 8 hours (this should not happen anyway, but just in case)
			if(start < lastDownloadTime + DOWNLOAD_LIMIT)
			{
				throw new IOException("Unexpected attempt to re-download file "
					+ "within 8 hours, which could cause access to be blocked");
			}
			// This second safety check handles attempts (when the connection
			// fails) to ensure that the attempt rate is not too frequent.
			if(start < lastAttemptTime + ATTEMPT_LIMIT)
			{
				throw new IOException("Unexpected attempt to re-download file "
					+ "within 1 minute, which could cause access to be blocked");
			}

			lastAttemptTime = start; // Now we're making an attempt
			InputStream remoteStream = openRemoteGzip();
			lastDownloadTime = start; // Now we've probably counted as a server hit
			in = new GZIPInputStream(remoteStream);
			out = new FileOutputStream(downloadFile);

			byte[] buffer = new byte[COPY_BUFFER];
			while(true)
			{
				int read = in.read(buffer);
				if(read == -1)
				{
					break;
				}
				out.write(buffer, 0, read);
			}

			ok = true;

			// Tell informer that file was downloaded
			informer.fileDownloaded((int)(getCurrentTime() - start),
				(int)downloadFile.length());
		}
		finally
		{
			// Delete file if there was an error
			if(!ok)
			{
				downloadFile.delete();
			}
			// Ensure streams are closed whatever the error status
			if(in != null)
			{
				try
				{
					in.close();
				}
				catch(Throwable t)
				{
				}
			}
			if(out != null)
			{
				try
				{
					out.close();
				}
				catch(Throwable t)
				{
				}
			}
		}
	}


	/**
	 * When a file has been downloaded and loaded successfully, it needs to
	 * be renamed to the real name so that it can be used in case of future
	 * application startups.
	 * <p>
	 * This method tries to safely rename it, with a high chance that there
	 * will be one file (either new or the old one if something goes wrong)
	 * however this method exits.
	 * <p>
	 * This is a separate method with default access so it can be used in unit
	 * testing.
	 * @throws IOException Any error renaming
	 */
	void renameDownloadedToReal() throws IOException
	{
		File downloadFile = getDownloadFile();
		File file = getFile();
		File oldFile = getOldFile();
		boolean exists = file.exists();
		if(oldFile.exists())
		{
			oldFile.delete();
		}
		if(exists && !file.renameTo(oldFile))
		{
			throw new IOException("Failed to rename existing file " + file);
		}
		if(!downloadFile.renameTo(file))
		{
			// Put old file back
			oldFile.renameTo(file);
			throw new IOException("Failed to rename new file to existing "
				+ file);
		}
		if(exists && !oldFile.delete())
		{
			throw new IOException("Failed to delete old file " + oldFile);
		}
	}

	/**
	 * Obtains the remote gzip file.
	 * <p>
	 * This is a separate method with default access so it can be used in unit
	 * testing.
	 * @return stream of data that will be the gzip-compressed file
	 * @throws IOException Any error opening stream
	 */
	InputStream openRemoteGzip() throws IOException
	{
		return downloadLocation.openStream();
	}
}
