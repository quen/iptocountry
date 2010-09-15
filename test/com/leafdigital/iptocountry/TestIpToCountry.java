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

import static org.junit.Assert.*;

import java.io.*;
import java.net.*;
import java.nio.charset.Charset;
import java.util.regex.Pattern;
import java.util.zip.GZIPOutputStream;

import org.junit.*;

/**
 * Test cases for {@link IpToCountry}.
 */
public class TestIpToCountry
{
	private String informerResults;
	private TestInformer testInformer;

	private File usedFolder;

	/**
	 * Clears local variables.
	 */
	@Before
	public void before()
	{
		informerResults = "";
		testInformer = new TestInformer();
		usedFolder = null;
	}

	/**
	 * Clears up temporary folder after test.
	 */
	@After
	public void after()
	{
		if(usedFolder != null)
		{
			File[] files = usedFolder.listFiles();
			if(files == null)
			{
				files = new File[0];
			}
			for(File file : files)
			{
				assertTrue(file.delete());
			}
			assertTrue(usedFolder.delete());
		}
	}

	/**
	 * Tests {@link IpToCountry#getCountryCode(long, long[], long[], String[])}.
	 */
	@Test
	public void testGetCountryCodeStatic()
	{
		long[] from, to;
		String[] codes;

		// Zero-item test
		from = new long[] { };
		to = new long[] { };
		codes = new String[] { };

		assertEquals("..", IpToCountry.getCountryCode(13, from, to, codes));

		// Single-item test dataset
		from = new long[] { 14 };
		to = new long[] { 17 };
		codes = new String[] { "QQ" };

		assertEquals("QQ", IpToCountry.getCountryCode(14, from, to, codes));
		assertEquals("QQ", IpToCountry.getCountryCode(17, from, to, codes));
		assertEquals("..", IpToCountry.getCountryCode(13, from, to, codes));
		assertEquals("..", IpToCountry.getCountryCode(18, from, to, codes));

		// Two-item test
		from = new long[] { 14, 93 };
		to = new long[] { 17, 99 };
		codes = new String[] { "AA", "BB" };
		assertEquals("AA", IpToCountry.getCountryCode(14, from, to, codes));
		assertEquals("AA", IpToCountry.getCountryCode(17, from, to, codes));
		assertEquals("BB", IpToCountry.getCountryCode(93, from, to, codes));
		assertEquals("BB", IpToCountry.getCountryCode(99, from, to, codes));
		assertEquals("..", IpToCountry.getCountryCode(13, from, to, codes));
		assertEquals("..", IpToCountry.getCountryCode(18, from, to, codes));
		assertEquals("..", IpToCountry.getCountryCode(92, from, to, codes));
		assertEquals("..", IpToCountry.getCountryCode(100, from, to, codes));

		// Many-item test
		from = new long[] { 10, 20, 30, 40, 50, 60, 70, 80, 90, 100 };
		to = new long[] { 19, 29, 39, 49, 59, 69, 79, 89, 99, 109 };
		codes = new String[] { "1", "2", "3", "4", "5", "6", "7", "8", "9", "10" };
		assertEquals("..", IpToCountry.getCountryCode(9, from, to, codes));
		assertEquals("..", IpToCountry.getCountryCode(110, from, to, codes));
		assertEquals("5", IpToCountry.getCountryCode(50, from, to, codes));
		assertEquals("7", IpToCountry.getCountryCode(79, from, to, codes));
		assertEquals("10", IpToCountry.getCountryCode(109, from, to, codes));
		assertEquals("1", IpToCountry.getCountryCode(10, from, to, codes));
		assertEquals("4", IpToCountry.getCountryCode(43, from, to, codes));
	}

	/**
	 * Tests {@link IpToCountry#getAddressAsLong(byte[])}.
	 */
	@Test
	public void testGetAddressAsLong()
	{
		assertEquals(0L, IpToCountry.getAddressAsLong(new byte[] { 0, 0, 0, 0 }));
		assertEquals(4294967295L, IpToCountry.getAddressAsLong(
			new byte[] { (byte)255, (byte)255, (byte)255, (byte)255 }));
		assertEquals(16909060L, IpToCountry.getAddressAsLong(
			new byte[] { 1, 2, 3, 4 }));
		try
		{
			IpToCountry.getAddressAsLong(new byte[5]);
			fail();
		}
		catch(IllegalArgumentException e)
		{
		}
	}

	/**
	 * Tests {@link IpToCountry#get4ByteAddress(InetAddress)}.
	 * @throws Exception
	 */
	@Test
	public void testGet4ByteAddress() throws Exception
	{
		assertArrayEquals(new byte[] {1, 2, 3, 4},
			IpToCountry.get4ByteAddress(InetAddress.getByName("1.2.3.4")));
		assertArrayEquals(new byte[] {101, 45, 75, (byte)219},
			IpToCountry.get4ByteAddress(InetAddress.getByName("::101.45.75.219")));
		assertArrayEquals(new byte[] {(byte)222, 1, 41, 90},
			IpToCountry.get4ByteAddress(InetAddress.getByName("::FFFF:222.1.41.90")));
		try
		{
			// IPv6 addresses not supported yet
			IpToCountry.get4ByteAddress(
				InetAddress.getByName("805B:2D9D:DC28::FC57:D4C8:1FFF"));
			fail();
		}
		catch(IllegalArgumentException e)
		{
		}
	}

	private class IpToCountryLocal extends IpToCountry
	{
		private long time;

		private IpToCountryLocal(File folder, String downloadLocation,
			long expireTime, Informer informer) throws IllegalArgumentException,
			MalformedURLException, IOException, InterruptedException
		{
			super(folder, downloadLocation, expireTime, informer);
		}

		private IpToCountryLocal(File folder, Informer informer)
			throws IllegalArgumentException, MalformedURLException, IOException,
			InterruptedException
		{
			super(folder, informer);
		}

		@Override
		InputStream openRemoteGzip() throws IOException
		{
			return TestIpToCountry.class.getResourceAsStream("IpToCountry.csv.gz");
		}

		@Override
		long getCurrentTime()
		{
			// Start with current time
			if(time == 0)
			{
				 time = System.currentTimeMillis();
			}
			return time;
		}

		void forwardTime(long delta)
		{
			time = getCurrentTime() + delta;
		}
	}

	private class IpToCountryArbitrary extends IpToCountryLocal
	{
		private byte[] data;
		private boolean downloadFail, loadFail;

		private IpToCountryArbitrary(File folder, Informer informer)
			throws IllegalArgumentException, MalformedURLException, IOException,
			InterruptedException
		{
			super(folder, informer);
		}

		@Override
		InputStream openRemoteGzip() throws IOException
		{
			if(downloadFail)
			{
				throw new IOException("fakeFail");
			}
			if(data == null)
			{
				// Default is an empty file
				setData("");
			}
			return new ByteArrayInputStream(data);
		}

		@Override
		void loadFile(File file) throws IOException
		{
			if(loadFail)
			{
				throw new IOException("fakeLoadFail");
			}
			super.loadFile(file);
		}

		void setData(String value) throws IOException
		{
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			GZIPOutputStream gzip = new GZIPOutputStream(out);
			gzip.write(value.getBytes(Charset.forName("US-ASCII")));
			gzip.close();
			data = out.toByteArray();
		}

		void setDownloadFail(boolean fail)
		{
			this.downloadFail = fail;
		}

		void setLoadFail(boolean fail)
		{
			this.loadFail = fail;
		}
	}

	private class TestInformer implements IpToCountry.Informer
	{
		@Override
		public void downloadFailed(Throwable t)
		{
			informerResults += "downloadFailed\n";
		}

		@Override
		public void loadFailed(Throwable t)
		{
			informerResults += "loadFailed\n";
		}

		@Override
		public void lineError(int errorIndex, String line)
		{
			informerResults += "lineError:" + errorIndex + ":" + line + "\n";
		}

		@Override
		public void fileDownloaded(int milliseconds, int bytes)
		{
			informerResults += "fileDownloaded:" + bytes + "\n";
		}

		@Override
		public void fileLoaded(int entries, int milliseconds, int lineErrors)
		{
			informerResults += "fileLoaded:" + entries + ":" + lineErrors + "\n";
		}
	}

	/**
	 * This is supposed to be a real-life test of constructing and using the
	 * system in a working example.
	 * @throws Exception Any problem
	 */
	@Test
	public void testRealExample() throws Exception
	{
		File folder = getFolder();

		// Init from scratch
		IpToCountryLocal ip = new IpToCountryLocal(folder, testInformer);
		String normalLoad = "fileDownloaded:7476549\nfileLoaded:104400:0\n";
		assertEquals(normalLoad, informerResults);
		informerResults = "";
		assertEquals("GB",
			ip.getCountryCode(InetAddress.getByName("212.58.246.92")));
		assertEquals("DE",
			ip.getCountryCode(InetAddress.getByName("195.71.11.67")));

		// Init from existing file
		ip = new IpToCountryLocal(folder, testInformer);
		assertEquals("fileLoaded:104400:0\n", informerResults);
		informerResults = "";
		assertEquals("GB",
			ip.getCountryCode(InetAddress.getByName("212.58.246.92")));

		// Init from existing file that's out of date
		long now = System.currentTimeMillis();
		ip.getFile().setLastModified(now - IpToCountry.EXPIRE_30DAYS - 1000);
		ip = new IpToCountryLocal(folder, testInformer);
		synchronized(ip)
		{
			// Should work immediately before download
			assertEquals("fileLoaded:104400:0\n", informerResults);
			informerResults = "";
			assertEquals("GB",
				ip.getCountryCode(InetAddress.getByName("212.58.246.92")));

			// But download should happen
			ip.wait(20000);
			assertEquals(normalLoad, informerResults);
			informerResults = "";

			// And after that it should still work
			assertEquals("GB",
				ip.getCountryCode(InetAddress.getByName("212.58.246.92")));
		}

		// Forward time and see if it downloads again
		ip.forwardTime(IpToCountry.EXPIRE_30DAYS + 1000);
		synchronized(ip)
		{
			// This one should return immediately
			assertEquals("GB",
				ip.getCountryCode(InetAddress.getByName("212.58.246.92")));
			assertEquals("", informerResults);

			// But then it should do a download
			ip.wait(20000);
			assertEquals(normalLoad, informerResults);
			informerResults = "";

			// And after that it should still work
			assertEquals("GB",
				ip.getCountryCode(InetAddress.getByName("212.58.246.92")));
		}
	}

	/**
	 * @return Temporary folder to use for test
	 * @throws IOException
	 */
	private File getFolder() throws IOException
	{
		File folder = File.createTempFile("iptocountry-unittest", null);
		folder.delete();
		folder.mkdir();
		usedFolder = folder;
		return folder;
	}

	/**
	 * Tests {@link IpToCountry#loadFile(File)}.
	 * @throws Exception
	 */
	@Test
	public void testLoadFile() throws Exception
	{
		File folder = getFolder();
		IpToCountryArbitrary ip = new IpToCountryArbitrary(folder, testInformer);
		assertEquals("fileDownloaded:0\nfileLoaded:0:0\n", informerResults);
		informerResults = "";

		// Test input using data that contains all types of line
		File test = new File(folder, "test.csv");
		String input =
			"#ignored\n"
			+ "\n"
			+ " also ignored\n"
			+ "invalid\n"
			+ "second invalid\n"
			+ "'1024','1024','whatever','0','AA',do not care about the rest\n"
			+ "'2048','2048','whatever','0','BB',do not care about the rest\n"
			+ "'3072','3072','whatever','0','AA',do not care about the rest\n";
		writeFile(test, input);
		ip.loadFile(test);
		assertEquals("lineError:0:invalid\nlineError:1:second invalid\nfileLoaded:3:2\n", informerResults);
		informerResults = "";

		// Check basic results
		assertEquals("..", ip.getCountryCode(InetAddress.getByName("0.0.4.1")));
		assertEquals("AA", ip.getCountryCode(InetAddress.getByName("0.0.4.0")));
		assertEquals("BB", ip.getCountryCode(InetAddress.getByName("0.0.8.0")));
		assertEquals("AA", ip.getCountryCode(InetAddress.getByName("0.0.12.0")));

		// Check string-sharing
		assertTrue(ip.getCountryCode(InetAddress.getByName("0.0.4.0"))
			== ip.getCountryCode(InetAddress.getByName("0.0.12.0")));
	}

	/**
	 * Writes a file to disk, replacing instances of ' with " (just to make
	 * it easier to type).
	 * @param file File
	 * @param contents Contents
	 * @throws IOException I/O error
	 */
	private void writeFile(File file, String contents) throws IOException
	{
		FileOutputStream out = new FileOutputStream(file);
		out.write(contents.replace('\'', '"').getBytes(Charset.forName("US-ASCII")));
		out.close();
	}

	/**
	 * Reads a file from disk, for checking.
	 * @param file File
	 * @return Contents as ASCII string
	 * @throws IOException I/O error
	 */
	private String readFile(File file) throws IOException
	{
		byte[] data = new byte[(int)file.length()];
		FileInputStream in = new FileInputStream(file);
		int pos = 0;
		while(pos < data.length)
		{
			int read = in.read(data, pos, data.length - pos);
			pos += read;
		}
		in.close();
		return new String(data, "US-ASCII");
	}

	/**
	 * Tests {@link IpToCountry#downloadFile()}.
	 * @throws Exception
	 */
	@Test
	public void testDownloadFile() throws Exception
	{
		// Initialise - this will download file once (successfully)
		File folder = getFolder();
		IpToCountryArbitrary ip = new IpToCountryArbitrary(folder, testInformer);
		assertEquals("fileDownloaded:0\nfileLoaded:0:0\n", informerResults);
		informerResults = "";

		// Try to make it download again within 8 hours
		ip.forwardTime(7 * 60 * 60 * 1000L);
		try
		{
			ip.downloadFile();
			fail();
		}
		catch(IOException e)
		{
		}

		// Try to make it download again after 8 hours
		ip.forwardTime(2 * 60 * 60 * 1000L);
		ip.downloadFile();
		assertEquals("fileDownloaded:0\n", informerResults);
		informerResults = "";

		// Now forward another 9 hours and repeat, but this time, the connection
		// should fail
		ip.forwardTime(9 * 60 * 60 * 1000L);
		ip.setDownloadFail(true);
		try
		{
			ip.downloadFile();
			fail();
		}
		catch(IOException e)
		{
			assertEquals("fakeFail", e.getMessage());
		}

		// Try again after 90 seconds - this should be allowed
		ip.forwardTime(90 * 1000L);
		try
		{
			ip.downloadFile();
			fail();
		}
		catch(IOException e)
		{
			assertEquals("fakeFail", e.getMessage());
		}

		// Try again after 30 seconds - this should be prevented
		ip.forwardTime(30 * 1000L);
		try
		{
			ip.downloadFile();
			fail();
		}
		catch(IOException e)
		{
			assertTrue(e.getMessage().contains("attempt to re-download file"));
		}

		// We should probably test that the download actually works, too
		// (this is already tested in other code but just in case)
		ip.forwardTime(90 * 1000L);
		ip.setDownloadFail(false);
		ip.setData("Hello world");
		ip.downloadFile();
		assertEquals("fileDownloaded:11\n", informerResults);
		informerResults = "";
		assertEquals("Hello world", readFile(ip.getDownloadFile()));
	}

	/**
	 * Tests {@link IpToCountry#renameDownloadedToReal()}.
	 * @throws Exception
	 */
	@Test
	public void testRenameDownloadedToReal() throws Exception
	{
		File folder = getFolder();
		IpToCountryArbitrary ip = new IpToCountryArbitrary(folder, testInformer);

		// Try successful rename
		writeFile(ip.getDownloadFile(), "New");
		writeFile(ip.getFile(), "Old");
		ip.renameDownloadedToReal();
		assertFalse(ip.getOldFile().exists());
		assertFalse(ip.getDownloadFile().exists());
		assertEquals("New", readFile(ip.getFile()));

		// Test if an old file got stuck there
		writeFile(ip.getDownloadFile(), "Newer");
		writeFile(ip.getOldFile(), "Old");
		ip.renameDownloadedToReal();
		assertFalse(ip.getOldFile().exists());
		assertFalse(ip.getDownloadFile().exists());
		assertEquals("Newer", readFile(ip.getFile()));

		// Test error if you don't actually have a downloaded file
		try
		{
			ip.renameDownloadedToReal();
			fail();
		}
		catch(IOException e)
		{
			assertTrue(e.getMessage().startsWith("Failed to rename new file"));
		}
		// Should be no change in file value
		assertEquals("Newer", readFile(ip.getFile()));

	}

	/**
	 * Tests the default informer output.
	 * @throws Exception
	 */
	@Test
	public void testDefaultInformer() throws Exception
	{
		ByteArrayOutputStream bytes = new ByteArrayOutputStream();
		PrintStream print = new PrintStream(bytes);
		PrintStream old = System.err;
		try
		{
			System.setErr(print);
			File folder = getFolder();

			// Test download and load output
			IpToCountryArbitrary ip = new IpToCountryArbitrary(folder, null);
			assertTrue(getOutput(print, bytes).matches(
				"IpToCountry: Downloaded new data file "
				+ "\\(0 bytes\\) in [0-9]+ ms\nIpToCountry: Loaded data file "
				+	"\\(0 entries\\) in "
				+ "[0-9]+ ms; 0 lines were not understood\n"));

			// Test download fails
			ip.forwardTime(IpToCountry.EXPIRE_30DAYS + 1);
			ip.setDownloadFail(true);
			synchronized(ip)
			{
				ip.getCountryCode(InetAddress.getByName("127.0.0.1"));
				ip.wait();
			}
			assertTrue(getOutput(print, bytes).matches(
				"IpToCountry: An error occurred when downloading from "
				+ Pattern.quote(IpToCountry.DOWNLOAD_LOCATION)
				+ ":\njava\\.io\\.IOException: fakeFail\n(.|\n)*"));

			// Test load fails
			ip.forwardTime(IpToCountry.EXPIRE_30DAYS + 1);
			ip.setDownloadFail(false);
			ip.setLoadFail(true);
			synchronized(ip)
			{
				ip.getCountryCode(InetAddress.getByName("127.0.0.1"));
				ip.wait();
			}
			assertTrue(getOutput(print, bytes).matches(
				"IpToCountry: Downloaded new data file "
				+ "\\(0 bytes\\) in [0-9]+ ms\n"
				+ "IpToCountry: An error occurred when loading the new data file "
				+ Pattern.quote(ip.getDownloadFile().toString())
				+ ":\njava\\.io\\.IOException: fakeLoadFail\n(.|\n)*"));

			// Test line errors
			ip.forwardTime(IpToCountry.EXPIRE_30DAYS + 1);
			ip.setLoadFail(false);
			ip.setData("# valid line\ninvalid\ntoo\n");
			synchronized(ip)
			{
				ip.getCountryCode(InetAddress.getByName("127.0.0.1"));
				ip.wait();
			}
			assertTrue(getOutput(print, bytes).matches(
				"IpToCountry: Downloaded new data file "
				+ "\\(25 bytes\\) in [0-9]+ ms\n"
				+ "IpToCountry: File line not understood \\(displaying first occurrence "
				+ "only\\):\ninvalid\n\n"
				+ "IpToCountry: Loaded data file "
				+	"\\(0 entries\\) in "
				+ "[0-9]+ ms; 2 lines were not understood\n"));
		}
		finally
		{
			System.setErr(old);
		}
	}

	private String getOutput(PrintStream print, ByteArrayOutputStream bytes)
		throws IOException
	{
		print.flush();
		String data = new String(bytes.toByteArray(), "US-ASCII");
		bytes.reset();
		return data;
	}

	private class IpToCountryFailDownload extends IpToCountryArbitrary
	{
		private IpToCountryFailDownload(File folder, IpToCountry.Informer informer)
			throws IllegalArgumentException, MalformedURLException, IOException,
				InterruptedException
		{
			super(folder, informer);
		}

		@Override
		InputStream openRemoteGzip() throws IOException
		{
			throw new IOException("forceFailDownload");
		}
	}

	private class IpToCountryFailLoad extends IpToCountryArbitrary
	{
		private IpToCountryFailLoad(File folder, IpToCountry.Informer informer)
			throws IllegalArgumentException, MalformedURLException, IOException,
				InterruptedException
		{
			super(folder, informer);
		}

		@Override
		void loadFile(File file) throws IOException
		{
			throw new IOException("forceFailLoad");
		}
	}

	/**
	 * Tests the constructor error-checking.
	 * @throws Exception
	 */
	@Test
	public void testConstructorErrors() throws Exception
	{
		// Folder with no access
		File folder = getFolder();
		folder.setWritable(false);
		try
		{
			new IpToCountryArbitrary(folder, testInformer);
			fail();
		}
		catch(IllegalArgumentException e)
		{
			assertTrue(e.getMessage().matches("Folder '.*' is not "
				+ "writable; check permissions"));
		}
		folder.setWritable(true);

		// Stupid expiry time
		try
		{
			new IpToCountry(folder, "http://www.example.org/", 37, testInformer);
			fail();
		}
		catch(IllegalArgumentException e)
		{
			assertEquals("Cannot set expire time lower than one day", e.getMessage());
		}

		// Error downloading
		try
		{
			new IpToCountryFailDownload(folder, testInformer);
			fail();
		}
		catch(IOException e)
		{
			assertEquals("forceFailDownload", e.getMessage());
		}

		// Error loading
		try
		{
			new IpToCountryFailLoad(folder, testInformer);
			fail();
		}
		catch(IOException e)
		{
			assertEquals("forceFailLoad", e.getMessage());
		}

		// Successful load so that we get a valid file in place
		IpToCountryArbitrary ip = new IpToCountryArbitrary(folder, testInformer);

		// Error downloading, but there is already a downloaded out-of-date copy
		ip.getFile().setLastModified(1);
		new IpToCountryFailDownload(folder, testInformer);
	}
}
