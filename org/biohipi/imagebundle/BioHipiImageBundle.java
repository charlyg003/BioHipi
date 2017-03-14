package org.biohipi.imagebundle;

import org.biohipi.image.BioHipiImageHeader;
import org.biohipi.image.io.CodecManager;
import org.biohipi.image.io.ImageDecoder;
import org.biohipi.image.BioHipiImageHeader.BioHipiImageFormat;
import org.biohipi.image.BioHipiImage;
import org.biohipi.util.ByteUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.EOFException;
import java.io.InputStream;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * A BioHipiImageBundle (BioHIB) is the primary representation for a
 * collection of biomedical images on the Hadoop Distributed File System (HDFS)
 * used by BioHIPI.
 *
 * BioHIBs are designed to take advantage of the fact that Hadoop
 * MapReduce is optimized to support efficient processing of large
 * flat files.
 *
 * This class provides basic methods for writing and reading of BioHIBs.
 * 
 * A single BioHIB is actually comprised of two files stored on the file
 * system: an index file and a data file. The index file contains a
 * list of byte offsets to the end of each image record (header
 * metadata + image pixel data) in the data file. The data file is
 * composed of a contiguous sequence of image records.
 *
 * @see <a href="http://hipi.cs.virginia.edu/">HIPI Project Homepage</a>
 */

public class BioHipiImageBundle {

	/**
	 * This FileReader enables reading individual images from a {@link
	 * org.biohipi.imagebundle.BioHipiImageBundle} and delivers them in the
	 * specified image type. This class is used by the {@link
	 * org.biohipi.imagebundle.mapreduce.BioHibInputFormat} and {@link
	 * org.biohipi.imagebundle.mapreduce.BioHibRecordReader} classes.
	 */
	public static class BioHibReader {

		// Input stream connected to HIB data file
		private DataInputStream dataInputStream = null;

		// Current position and start/end offsets in input stream
		private long currentOffset = 0;
		private long startOffset = 0;
		private long endOffset = 0;

		// Each image record in the data file begins with a 12 byte
		// "signature" that indicates length of header, length of image
		// data, and image storage format in that order
		private byte sig[] = new byte[12];

		// Current image, accessed with calls to getCurrentKey and
		// getCurrentValue
		private BioHipiImageFormat imageFormat = BioHipiImageFormat.UNDEFINED;
		private byte[] imageBytes = null;
		private BioHipiImageHeader imageHeader = null;
		private BioHipiImage image = null;

		/**
		 * Creates a BioHibReader to read records (image headers / image
		 * bodies) from a contiguous segment (file split) of a BioHIB data
		 * file. The segment is specified by a start and end byte offset.
		 * 
		 * @param fs The {@link FileSystem} where the BioHIB data file resides
		 * @param path The {@link Path} to the BioHIB data file
		 * @param start The byte offset to beginning of segment
		 * @param end The byte offset to end of segment
		 *
		 * @throws IOException
		 */
		public BioHibReader(FileSystem fs, Path path, long start, long end) throws IOException {

			// Create input stream for BioHIB data file
			dataInputStream = new DataInputStream(fs.open(path));

			// Advance input stream to requested start byte offset. This may
			// take several calls to the DataInputStream.skip() method.
			startOffset = start;
			while (start > 0) {
				long skipped = dataInputStream.skip(start);
				if (skipped <= 0) {
					break;
				}
				start -= skipped;
			}

			// Store current byte offset along with end byte offset
			currentOffset = startOffset;
			endOffset = end;
		}

		public BioHibReader(FileSystem fs, Path path) throws IOException {
			this(fs, path, 0, 0); // endOffset = 0 indicates read until EOF
		}

		/**
		 * Returns current amount of progress reading file.
		 * 
		 * @return Measure of progress from 0.0 (no progress) to 1.0
		 * (finished).
		 */
		public float getProgress() {
			float progress = (endOffset - startOffset + 1) > 0 ? (float) (currentOffset - startOffset) / (float) (endOffset - startOffset + 1) : 0.f;
			// Clamp to handle rounding errors
			if (progress > 1.f) {
				return 1.f;
			} else if (progress < 0.f) {
				return 0.f;
			}
			return progress;
		}

		/**
		 * Closes any open objects used to read the BioHIB data file (e.g.,
		 * DataInputStream).
		 */
		public void close() throws IOException {
			if (dataInputStream != null) {
				dataInputStream.close();
			}
		}

		/**
		 * Reads the next image header and image body into memory. To
		 * obtain the corresponding {@link org.biohipi.image.BioHipiImageHeader} and {@link
		 * org.biohipi.image.RasterImage} objects, call {@link #getCurrentKey()} and {@link
		 * #getCurrentValue()} respectively.
		 * 
		 * @return true if the next image record (header + pixel data) was successfully read and decoded. False if there are no more images or if an error occurs.
		 */
		public boolean nextKeyValue() {
			try {

				// Reset state of current key/value
				imageFormat = BioHipiImageFormat.UNDEFINED;
				imageBytes = null;
				imageHeader = null;
				image = null;

				// A value of endOffset = 0 indicates "read to the end of
				// file", otherwise check segment boundary
				if (endOffset > 0 && currentOffset > endOffset) {
					// Already past end of file segment
					return false;
				}

				// Attempt to read 12-byte signature that contains length of
				// image header, length of image data segment, and image
				// storage format

				int sigOffset = 0;
				int bytesRead = dataInputStream.read(sig);

				// Even reading signature might require multiple calls
				while (bytesRead < (sig.length - sigOffset) && bytesRead > 0) {
					sigOffset += bytesRead;
					bytesRead = dataInputStream.read(sig, sigOffset, sig.length - sigOffset);
				}

				if (bytesRead <= 0) {
					// Reached end of file without error
					return false;
				}

				if (bytesRead < sig.length) {
					// Read part of signature before encountering EOF. Malformed file.
					throw new IOException(String.format("Failed to read %d-byte BioHIB image signature that delineates image record boundaries.", sig.length));
				}

				// Parse and validate image header length
				int imageHeaderLength = ((sig[0] & 0xff) << 24) | ((sig[1] & 0xff) << 16) | ((sig[2] & 0xff) << 8) | (sig[3] & 0xff);
				if (imageHeaderLength <= 0) {
					// Negative or zero file length, report corrupted BioHIB
					throw new IOException("Found image header length <= 0 in BioHIB at offset: " + currentOffset);
				}

				// Parse and validate image length
				int imageLength = ((sig[4] & 0xff) << 24) | ((sig[5] & 0xff) << 16) | ((sig[6] & 0xff) << 8) | (sig[7] & 0xff);
				if (imageLength <= 0) {
					// Negative or zero file length, report corrupted BioHIB
					throw new IOException("Found image data segment length <= 0 in BioHIB at offset: " + currentOffset);
				}

				// Parse and validate image format
				int imageFormatInt = ((sig[8] & 0xff) << 24) | ((sig[9] & 0xff) << 16) | ((sig[10] & 0xff) << 8) | (sig[11] & 0xff);
				try {
					imageFormat = BioHipiImageFormat.fromInteger(imageFormatInt);
				} catch (IllegalArgumentException e) {
					throw new IOException("Found invalid image storage format in BioHIB at offset: " + currentOffset);
				}
				if (imageFormat == BioHipiImageFormat.UNDEFINED) {
					throw new IOException("Found UNDEFINED image storage format in BioHIB at offset: " + currentOffset);
				}

				// Allocate byte array to hold image header data
				byte[] imageHeaderBytes = new byte[imageHeaderLength];

				// Allocate byte array to hold image data
				imageBytes = new byte[imageLength];

				// an exception? The stream position will become out of sync
				// with currentOffset.
				dataInputStream.readFully(imageHeaderBytes);        
				dataInputStream.readFully(imageBytes);        

				// Advance byte offset by length of 12-byte signature plus
				// image header length plus image pixel data length
				currentOffset += 12 + imageHeaderLength + imageLength;

				// Attempt to decode image header
				DataInputStream dis = new DataInputStream(new ByteArrayInputStream(imageHeaderBytes));
				imageHeader = new BioHipiImageHeader(dis);

				System.out.println("MetaInfo: " + imageHeader.getAllMetaData());

				// Wrap image bytes in stream
				ByteArrayInputStream imageByteStream = new ByteArrayInputStream(imageBytes);

				// Obtain suitable image decoder
				ImageDecoder decoder = CodecManager.getDecoder(imageFormat);
				if (decoder == null) {
					throw new IOException("Unsupported storage format in image record ending at byte offset: " + currentOffset);
				}

				// Call appropriate decode function based on type of image
				// object
				try {
					image = decoder.decodeImage(imageByteStream, imageHeader);
				} catch (Exception e) {
					System.err.println("Runtime exception while attempting to decode image: " + e.getMessage());
					e.printStackTrace();
					// Attempt to keep going
					return nextKeyValue();
				}

				return true;

			} catch (EOFException e) {
				System.err.println(String.format("EOF exception [%s] while decoding BioHIB image record ending at byte offset [%d]", 
						e.getMessage(), currentOffset, endOffset));
				e.printStackTrace();
				imageFormat = BioHipiImageFormat.UNDEFINED;
				imageBytes = null;
				imageHeader = null;
				image = null;
				return false;
			} catch (IOException e) {
				System.err.println(String.format("IO exception [%s] while decoding BioHIB image record ending at byte offset [%d]",
						e.getMessage(), currentOffset));
				e.printStackTrace();
				imageFormat = BioHipiImageFormat.UNDEFINED;
				imageBytes = null;
				imageHeader = null;
				image = null;
				return false;
			} catch (RuntimeException e) {
				System.err.println(String.format("Runtime exception [%s] while decoding BioHIB image record ending at byte offset [%d]",
						e.getMessage(), currentOffset));
				e.printStackTrace();
				imageFormat = BioHipiImageFormat.UNDEFINED;
				imageBytes = null;
				imageHeader = null;
				image = null;
				return false;
			} catch (Exception e) {
				System.err.println(String.format("Unexpected exception [%s] while decoding BioHIB image record ending at byte offset [%d]",
						e.getMessage(), currentOffset));
				e.printStackTrace();
				imageFormat = BioHipiImageFormat.UNDEFINED;
				imageBytes = null;
				imageHeader = null;
				image = null;
				return false;
			}

		}

		/**
		 * @return Byte array containing raw image data.
		 */
		public byte[] getImageBytes() {
			return imageBytes;
		}

		/**
		 * @return Storage format of raw image bytes.
		 */
		public BioHipiImageFormat getImageStorageFormat() {
			return imageFormat;
		}

		/**
		 * @return Header for the current image, as retrieved by {@link
		 * #nextKeyValue()}
		 */
		public BioHipiImageHeader getCurrentKey() {
			return imageHeader;
		}

		/**
		 * @return Current decoded image, as retrieved by {@link
		 * #nextKeyValue()}
		 */
		public BioHipiImage getCurrentValue() {
			return image;
		}

	} // public static class HibReader

	public static final int FILE_MODE_UNDEFINED = 0;
	public static final int FILE_MODE_READ = 1;
	public static final int FILE_MODE_WRITE = 2;

	private int fileMode = FILE_MODE_UNDEFINED;

	private Path indexFilePath = null;
	private Path dataFilePath = null;

	protected Configuration conf = null;

	//	protected BioHipiImageFactory imageFactory = null;

	private DataInputStream indexInputStream = null;
	private DataOutputStream indexOutputStream = null;
	private DataOutputStream dataOutputStream = null;

	private BioHibReader hibReader = null;

	private byte sig[] = new byte[12];

	private long currentOffset = 0;

	private long blockSize = 0;
	private short replication = 0;

	public BioHipiImageBundle(Path indexFilePath, Configuration conf) {
		this.indexFilePath = indexFilePath;
		this.dataFilePath = indexFilePath.suffix(".dat");
		this.conf = conf;
	}

	public BioHipiImageBundle(Path indexFilePath, Configuration conf, short replication) {
		this(indexFilePath, conf);
		this.replication = replication;
	}

	public BioHipiImageBundle(Path indexFilePath, Configuration conf, long blockSize) {
		this(indexFilePath, conf);
		this.blockSize = blockSize;
	}

	public BioHipiImageBundle(Path indexFilePath, Configuration conf, short replication, long blockSize) {
		this(indexFilePath, conf);
		this.replication = replication;
		this.blockSize = blockSize;
	}

	public Path getPath() {
		return indexFilePath;
	}

	/**
	 * Opens the underlying index and data files for writing.
	 * 
	 * @param overwrite if either part of the BioHIB file (index and/or data) exists this parameter determines whether or not to delete the file first or throw an exception
	 *
	 * @throws IOException in the event of any I/O errors while creating and opening the index and data files for subsequent writing
	 */
	public final void openForWrite(boolean overwrite) throws IOException {

		if (fileMode != FILE_MODE_UNDEFINED) {
			throw new IOException("BioHIB [" + indexFilePath.getName() + "] is already open. Must close before calling this method.");
		}

		FileSystem fs = FileSystem.get(conf);

		if (fs.exists(indexFilePath) && !overwrite) {
			throw new IOException("BioHIB [" + indexFilePath.getName() + "] already exists. Cannot open HIB for writing unless overwrite is specified.");
		}

		if (fs.exists(dataFilePath) && !overwrite) {
			throw new IOException("BioHIB [" + dataFilePath.getName() + "] already exists. Cannot open HIB for writing unless overwrite is specified.");
		}

		assert indexOutputStream == null;
		assert dataOutputStream == null;
		assert indexInputStream == null;

		if (blockSize <= 0) {
			blockSize = fs.getDefaultBlockSize(dataFilePath);
		}

		if (replication <= 0) {
			replication = fs.getDefaultReplication(dataFilePath);
		}
		try {
			indexOutputStream = new DataOutputStream(fs.create(indexFilePath));
			dataOutputStream = new DataOutputStream(fs.create(dataFilePath, true, fs.getConf().getInt("io.file.buffer.size", 4096), replication, blockSize));
			currentOffset = 0;
			writeBundleHeader();
		} catch (IOException ex) {
			System.err.println("I/O exception while attempting to open BioHIB [" + indexFilePath.getName() + "] for writing with overwrite [" + overwrite + "].");
			System.err.println(ex.getMessage());
			indexOutputStream = null;
			dataOutputStream = null;
			indexInputStream = null;
			return;
		}

		// Indicates success
		fileMode = FILE_MODE_WRITE;
	}

	/**
	 * BioHIB index file header structure:
	 * BOF
	 * 4 bytes (int): magic signature (0x81911b18) "HIPIIbIH"
	 * 2 bytes (short int): length of data file name
	 * var bytes: data file path name
	 * 16 bytes: reserved for future use
	 * 4 bytes: number of bytes to skip to reach start of offset list
	 * [8 byte]*: offsets
	 * EOF
	 */
	private void writeBundleHeader() throws IOException {
		assert indexOutputStream != null;
		// Magic number
		indexOutputStream.writeInt(0x81911b18);
		// Reserved fields (16 bytes)
		indexOutputStream.writeLong(0);
		indexOutputStream.writeLong(0);
		// Number of bytes to skip (0)
		indexOutputStream.writeInt(0);
	}

	/**
	 * Add image to the BioHIB. This involves appending the image to the data file, and adding the corresponding byte offset to the index file.
	 *
	 * @param imageHeader initialized image header
	 * @param imageStream input stream containing the image data. This data is not decoded or verified to be consistent with the provided image header. It is simply appended to the BioHIB data file.
	 *
	 * @throws IOException in the event of any I/O errors or if the BioHIB is not currently in a state that supports adding new images
	 */
	public void addImage(BioHipiImageHeader imageHeader, InputStream imageStream) throws IOException {

		if (fileMode != FILE_MODE_WRITE) {
			throw new IOException("BioHIB [" + indexFilePath.getName() + "] is not opened for writing. Must successfully open BioHIB for writing before calling this method.");
		}

		// Serialize imageHeader into byte[]
		ByteArrayOutputStream imageHeaderStream = new ByteArrayOutputStream(1024);
		imageHeader.write(new DataOutputStream(imageHeaderStream));
		byte imageHeaderBytes[] = imageHeaderStream.toByteArray();
		int imageHeaderLength = imageHeaderBytes.length;

		// Read image input stream and convert to byte[]
		byte imageBytes[] = ByteUtils.inputStreamToByteArray(imageStream);
		int imageLength = imageBytes.length;

		int imageFormatInt = imageHeader.getStorageFormat().toInteger();

		sig[0] = (byte) ((imageHeaderLength >> 24));
		sig[1] = (byte) ((imageHeaderLength >> 16) & 0xff);
		sig[2] = (byte) ((imageHeaderLength >> 8) & 0xff);
		sig[3] = (byte) ((imageHeaderLength) & 0xff);

		sig[4] = (byte) ((imageLength >> 24));
		sig[5] = (byte) ((imageLength >> 16) & 0xff);
		sig[6] = (byte) ((imageLength >> 8) & 0xff);
		sig[7] = (byte) ((imageLength) & 0xff);

		sig[8] = (byte) ((imageFormatInt >> 24));
		sig[9] = (byte) ((imageFormatInt >> 16) & 0xff);
		sig[10] = (byte) ((imageFormatInt >> 8) & 0xff);
		sig[11] = (byte) ((imageFormatInt) & 0xff);

		dataOutputStream.write(sig);
		dataOutputStream.write(imageHeaderBytes);
		dataOutputStream.write(imageBytes);
		
		currentOffset += 12 + imageHeaderLength + imageLength;
		indexOutputStream.writeLong(currentOffset);
	}

	public void addImage(InputStream inputStream, BioHipiImageFormat imageFormat, HashMap<String, String> metaData) throws IllegalArgumentException, IOException {

		ImageDecoder decoder = CodecManager.getDecoder(imageFormat);
		
		byte imageBytes[] = ByteUtils.inputStreamToByteArray(inputStream);

		BioHipiImageHeader header = decoder.decodeHeader(new ByteArrayInputStream(imageBytes));

		if (metaData != null)
			header.appendMetaData(metaData);

		addImage(header, new ByteArrayInputStream(imageBytes));

	}

	public void addImage(InputStream inputStream, BioHipiImageFormat imageFormat) throws IllegalArgumentException, IOException {
		addImage(inputStream, imageFormat, null);
	}

	public void openForRead(int seekToImageIndex) throws IOException, IllegalArgumentException {

		if (seekToImageIndex < 0) {
			throw new IllegalArgumentException("Image index must be non-negative [" + seekToImageIndex + "].");
		}

		if (fileMode != FILE_MODE_UNDEFINED) {
			throw new IOException("BioHIB [" + indexFilePath.getName() + "] is already open. Must close before calling this method.");
		}

		FileSystem fs = FileSystem.get(conf);

		if (!fs.exists(indexFilePath)) {
			throw new IOException("BioHIB index file not found while attempting open for read [" + indexFilePath.getName() + "].");
		}

		if (!fs.exists(dataFilePath)) {
			throw new IOException("BioHIB data file not found while attempting open for read [" + dataFilePath.getName() + "].");
		}

		assert indexOutputStream == null;
		assert dataOutputStream == null;
		assert indexInputStream == null;

		List<Long> offsets = null;

		try {
			if (seekToImageIndex == 0) {
				indexInputStream = new DataInputStream(fs.open(indexFilePath));
				readBundleHeader();
				hibReader = new BioHibReader(fs, dataFilePath);
			} else {
				// Attempt to seek to desired image position
				indexInputStream = new DataInputStream(fs.open(indexFilePath));
				readBundleHeader();
				offsets = readOffsets(seekToImageIndex);
				if (offsets.size() == seekToImageIndex) {
					hibReader = new BioHibReader(fs, dataFilePath, offsets.get(offsets.size()-1), 0);
				}
			}      
		} catch (IOException ex) {
			System.err.println("I/O exception while attempting to open BioHIB [" + indexFilePath.getName() + "] for reading.");
			System.err.println(ex.getMessage());
			indexOutputStream = null;
			dataOutputStream = null;
			indexInputStream = null;
			return;
		}

		if (seekToImageIndex != 0) {
			if (offsets == null) {
				throw new IOException("Failed to read file offsets for BioHIB [" + indexFilePath.getName() + "].");
			}
			if (offsets.size() != seekToImageIndex) {
				throw new IOException("Failed to seek to image index [" + seekToImageIndex + "]. Check that it is not past end of file.");
			}
		}

		// Indicates success
		fileMode = FILE_MODE_READ;
	}

	public void openForRead() throws IOException {
		openForRead(0);
	}

	private void readBundleHeader() throws IOException {
		assert indexInputStream != null;

		// Verify signature
		int sig = indexInputStream.readInt();
		if (sig != 0x81911b18) {
			throw new IOException("Corrupted BioHIB header: signature mismatch.");
		}

		// Use readLong to skip reserved fields instead of skip because
		// skip doesn't guarantee success. If readLong reaches EOF will
		// throw exception.
		indexInputStream.readLong();
		indexInputStream.readLong();

		int skipOver = indexInputStream.readInt();
		while (skipOver > 0) {
			long skipped = indexInputStream.skip(skipOver);
			if (skipped <= 0) {
				break;
			}
			skipOver -= skipped;
		}
	}

	/**
	 * 
	 * @return a {@link List} of image offsets
	 */
	public List<Long> readAllOffsets() {
		return readOffsets(0);
	}

	/**
	 * @return The data file for the HipiImageBundle
	 */
	public FileStatus getDataFileStatus() throws IOException {
		return FileSystem.get(conf).getFileStatus(dataFilePath);
	}

	/**
	 * Attemps to read some number of image record offsets from the BioHIB
	 * index file.
	 *
	 * @param maximumNumber the maximum number of offsets that will be
	 *        read from the BioHIB index file. The actual number read may
	 *        be less than this number.
	 * @return A list of file offsets read from the BioHIB index file.
	 */
	public List<Long> readOffsets(int maximumNumber) {
		ArrayList<Long> offsets = new ArrayList<Long>(maximumNumber);
		for (int i = 0; i < maximumNumber || maximumNumber == 0; i++) {
			try {
				offsets.add(indexInputStream.readLong());
			} catch (IOException e) {
				break;
			}
		}
		return offsets;
	}

	public boolean next() throws IOException {
		if (fileMode != FILE_MODE_READ) {
			throw new IOException("BioHIB [" + indexFilePath.getName() + "] is not opened for reading. Must successfully open BioHIB for reading before calling this method.");
		}
		assert hibReader != null;
		return hibReader.nextKeyValue();
	}

	/**
	 * @see BioHipiImageBundle.BioHibReader#getCurrentKey()
	 */
	public BioHipiImageHeader currentHeader() throws IOException {
		if (fileMode != FILE_MODE_READ) {
			throw new IOException("BioHIB [" + indexFilePath.getName() + "] is not opened for reading. Must successfully open BioHIB for reading before calling this method.");
		}
		assert hibReader != null;
		return hibReader.getCurrentKey();
	}

	/**
	 * @see BioHipiImageBundle.BioHibReader#getCurrentValue()
	 */
	public BioHipiImage currentImage() throws IOException {
		if (fileMode != FILE_MODE_READ) {
			throw new IOException("BioHIB [" + indexFilePath.getName() + "] is not opened for reading. Must successfully open BioHIB for reading before calling this method.");
		}
		assert hibReader != null;
		return hibReader.getCurrentValue();
	}

	public void close() throws IOException {

		if (hibReader != null) {
			hibReader.close();
			hibReader = null;
		}

		if (indexInputStream != null) {
			indexInputStream.close();
			indexInputStream = null;
		}

		if (dataOutputStream != null) {
			dataOutputStream.close();
			dataOutputStream = null;
		}

		if (indexOutputStream != null) {
			indexOutputStream.close();
			indexOutputStream = null;
		}

		fileMode = FILE_MODE_UNDEFINED;
	}

	/**
	 * Appends another BioHIB to the current BioHIB. This involves concatenating the underlying data files and index files.
	 * 
	 * @param bundle target BioHIB to be appended to the current BioHIB
	 */
	public void append(BioHipiImageBundle bundle) {
		try {
			bundle.openForRead();
			FileStatus dataFileStatus = bundle.getDataFileStatus();
			List<Long> offsets = bundle.readAllOffsets();

			// Concatenate data file
			FileSystem fs = FileSystem.get(conf);
			DataInputStream dataInputStream = new DataInputStream(fs.open(dataFileStatus.getPath()));
			int numBytesRead = 0;
			byte[] data = new byte[1024 * 1024]; // Transfer in 1MB blocks
			while ((numBytesRead = dataInputStream.read(data)) > -1) {
				dataOutputStream.write(data, 0, numBytesRead);
			}
			dataInputStream.close();

			// Concatenate index file
			long lastOffset = currentOffset;
			for (int j = 0; j < offsets.size(); j++) {
				currentOffset = (long) (offsets.get(j)) + lastOffset;
				indexOutputStream.writeLong(currentOffset);
			}

			// Clean up
			dataOutputStream.flush();
			indexOutputStream.flush();
			bundle.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

}
