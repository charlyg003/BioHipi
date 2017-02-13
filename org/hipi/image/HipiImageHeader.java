package org.hipi.image;

import org.apache.hadoop.io.WritableComparable;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * The header information for a HipiImage. HipiImageHeader encapsulates universally available
 * information about a 2D image: width, height, storage format, color space, number of color bands 
 * (also called "channels") along with optional image meta data and EXIF data represented
 * as key/value String dictionaries.
 *
 * The {@link org.hipi.image.io} package provides classes for reading (decoding) HipiImageHeader
 * from both {@link org.hipi.imagebundle.HipiImageBundle} files and various standard image storage
 * formats such as JPEG and PNG.
 *
 * Note that this class implements the {@link org.apache.hadoop.io.WritableComparable} interface,
 * allowing it to be used as a key/value object in MapReduce programs.
 */
public class HipiImageHeader implements WritableComparable<HipiImageHeader> {

	/**
	 * Enumeration of the image storage formats supported in HIPI (JPEG, PNG, NIFTI, RDA, DICOM).
	 */
	public enum HipiImageFormat {
		UNDEFINED(0x0), JPEG(0x1), PNG(0x2), NIFTI(0x3), RDA(0x4), DICOM(0x5);

		private int format;

		/**
		 * Creates an ImageFormat from an int.
		 *
		 * @param format Integer representation of ImageFormat.
		 */
		HipiImageFormat(int format) {
			this.format = format;
		}

		/**
		 * Creates an ImageFormat from an int.
		 *
		 * @param format Integer representation of ImageFormat.
		 *
		 * @return Associated ImageFormat.
		 *
		 * @throws IllegalArgumentException if the parameter value does not correspond to a valid
		 * HipiImageFormat.
		 */
		public static HipiImageFormat fromInteger(int format) throws IllegalArgumentException {
			for (HipiImageFormat fmt : values()) {
				if (fmt.format == format) {
					return fmt;
				}
			}
			throw new IllegalArgumentException(String.format("There is no HipiImageFormat enum value " +
					"associated with integer [%d]", format));
		}

		/** 
		 * @return Integer representation of ImageFormat.
		 */
		public int toInteger() {
			return format;
		}

		/**
		 * Default HipiImageFormat.
		 *
		 * @return HipiImageFormat.UNDEFINED
		 */
		public static HipiImageFormat getDefault() {
			return UNDEFINED;
		}

	} // public enum ImageFormat

	/**
	 * Enumeration of the color spaces supported in HIPI.
	 */
	public enum HipiColorSpace {
		UNDEFINED(0x0), RGB(0x1), LUM(0x2);

		private int cspace;

		/**
		 * Creates a HipiColorSpace from an int
		 *
		 * @param format Integer representation of ColorSpace.
		 */
		HipiColorSpace(int cspace) {
			this.cspace = cspace;
		}

		/**
		 * Creates a HipiColorSpace from an int.
		 *
		 * @param cspace Integer representation of ColorSpace.
		 *
		 * @return Associated HipiColorSpace value.
		 *
		 * @throws IllegalArgumentException if parameter does not correspond to a valid HipiColorSpace.
		 */
		public static HipiColorSpace fromInteger(int cspace) throws IllegalArgumentException {
			for (HipiColorSpace cs : values()) {
				if (cs.cspace == cspace) {
					return cs;
				}
			}
			throw new IllegalArgumentException(String.format("There is no HipiColorSpace enum value " +
					"with an associated integer value of %d", cspace));
		}

		/** 
		 * Integer representation of ColorSpace.
		 *
		 * @return Integer representation of ColorSpace.
		 */
		public int toInteger() {
			return cspace;
		}

		/**
		 * Default HipiColorSpace. Currently (linear) RGB.
		 *
		 * @return Default ColorSpace enum value.
		 */
		public static HipiColorSpace getDefault() {
			return RGB;
		}

	} // public enum ColorSpace

	/**
	 * Enumeration of the key image informations supported in {@link HipiImageHeader#imageInfo} map.<br>
	 * <p><b>Jpeg/Png format:</b><pre><i>COLOR_SPACE, WIDTH, HEIGHT, BANDS</i>;<br></pre>
	 * <b>NIfTI format:</b><pre><i>X_LENGTH, Y_LENGTH, Z_LENGTH, T_LENGTH</i>;<br></pre>
	 * <b>DICOM format:</b><pre><i>PATIENT_ID, PATIENT_NAME, ROWS, COLUMNS</i>;<br></pre>
	 */
	public enum HipiKeyImageInfo {
		/** default value */ 									UNDEFINED(0x0),

		/** {@link HipiColorSpace} of the Jpeg or Png Image */ 	COLOR_SPACE(0x1), 
		/** width of the Jpeg or Png Image */ 					WIDTH(0x2),
		/** height of the Jpeg or Png Image */ 					HEIGHT(0x3),
		/** number of Bands of the Jpeg or Png Image */ 		BANDS(0x4),

		/** x-axis length of the NIfTI image */ 				X_LENGTH(0x5),
		/** y-axis length of the NIfTI image */ 				Y_LENGTH(0x6),
		/** z-axis length of the NIfTI image */ 				Z_LENGTH(0x7),
		/** t-axis length of the NIfTI image */ 				T_LENGTH(0x8),

		/** patient id of the DICOM image */ 					PATIENT_ID(0x9),
		/** patient name of the DICOM image */ 					PATIENT_NAME(0x10),
		/** rows of the DICOM image */ 							ROWS(0x11),
		/** columns of the DICOM image */ 						COLUMNS(0x12);

		private int key;

		/**
		 * Creates a HipiKeyImageInfo from an int
		 *
		 * @param format Integer representation of KeyImageInfo.
		 */
		HipiKeyImageInfo(int key) {
			this.key = key;
		}

		/**
		 * Creates a HipiKeyImageInfo from an int.
		 *
		 * @param key Integer representation of KeyImageInfo.
		 *
		 * @return Associated HipiKeyImageInfo value.
		 *
		 * @throws IllegalArgumentException if parameter does not correspond to a valid HipiKeyImageInfo.
		 */
		public static HipiKeyImageInfo fromInteger(int key) throws IllegalArgumentException {
			for (HipiKeyImageInfo k : values()) {
				if (k.key == key) {
					return k;
				}
			}
			throw new IllegalArgumentException(String.format("There is no HipiKeyImageInfo enum value " +
					"with an associated integer value of %d", key));
		}

		/** 
		 * Integer representation of KeyImageInfo.
		 *
		 * @return Integer representation of KeyImageInfo.
		 */
		public int toInteger() {
			return key;
		}

		/**
		 * Default HipiKeyImageInfo. Currently (linear) UNDEFINED.
		 *
		 * @return Default KeyImageInfo enum value.
		 */
		public static HipiKeyImageInfo getDefault() {
			return UNDEFINED;
		}

	} // public enum HipiKeyImageInfo
	
	/**
	 * Format used to store image on HDFS
	 */
	private HipiImageFormat storageFormat;
	
	/**
	 * A map containing key/value pairs of information
	 * associated with the image.
	 * 
	 * @see HipiKeyImageInfo
	 */
	private Map<HipiKeyImageInfo, Object> imageInfo = new HashMap<HipiKeyImageInfo, Object>();

	/**
	 * A map containing key/value pairs of meta data associated with the
	 * image. These are (optionally) added during HIB construction and
	 * are distinct from the exif data that may be stored within the
	 * image file, which is accessed through the IIOMetadata object. For
	 * example, this would be the correct place to store the image tile
	 * offset and size if you were using a HIB to store a very large
	 * image as a collection of smaller image tiles. Another example
	 * would be using this dictionary to store the source url for an
	 * image downloaded from the Internet.
	 */
	private Map<String, String> metaData = new HashMap<String,String>();

	/**
	 * EXIF data associated with the image represented as a
	 * HashMap. {@see hipi.image.io.ExifDataUtils}
	 */
	private Map<String, String> exifData = new HashMap<String,String>();

	/**
	 * Creates an ImageHeader default.
	 */
	public HipiImageHeader(HipiImageFormat storageFormat, byte[] metaDataBytes, Map<String,String> exifData) {
		this.storageFormat = storageFormat;

		if (metaDataBytes != null)
			setMetaDataFromBytes(metaDataBytes);

		this.exifData = exifData;
	}

	/**
	 * Creates an ImageHeader with image information.
	 */
	public HipiImageHeader(HipiImageFormat storageFormat, Map<HipiKeyImageInfo, Object> imgInfo, byte[] metaDataBytes, Map<String,String> exifData) {
		this(storageFormat, metaDataBytes, exifData);
		this.imageInfo = new HashMap<HipiKeyImageInfo, Object>(imgInfo);
	}

	/**
	 * Creates an ImageHeader by calling #readFields on the data input
	 * object. Note that this function does not populate the exifData
	 * field. That must be done with a separate method call.
	 */
	public HipiImageHeader(DataInput input) throws IOException {
		readFields(input);
	}

	/**
	 * Get the image storage type.
	 *
	 * @return Current image storage type.
	 */
	public HipiImageFormat getStorageFormat() {
		return storageFormat;
	}

	/**
	 * Get the entire map of image informations.
	 *
	 * @return a hash map containing the keys and values of the image informations
	 */
	public HashMap<HipiKeyImageInfo, Object> getAllImageInfo() {
		return new HashMap<HipiKeyImageInfo, Object>(imageInfo);
	}

	/**
	 * Attempt to retrieve image informations value associated with key.
	 * 
	 * @param key field name of the desired information record
	 * @return either the value corresponding to the key or null if the
	 * key was not found
	 * 
	 * @see HipiKeyImageInfo
	 */
	public Object getImageInfo(HipiKeyImageInfo key) {
		return this.imageInfo.get(key);
	}

	/**
	 * Adds an image information field to this header object. The information consists of a
	 * key-value pair where the key is an {@link HipiKeyImageInfo} enumeration field name and the 
	 * value is the corresponding information for that field.
	 * 
	 * @param key {@link HipiKeyImageInfo} field name
	 * @param value image information
	 * 
	 * @see HipiKeyImageInfo
	 */
	public void addImageInfo(HipiKeyImageInfo key, Object value) {
		this.imageInfo.put(key, value);
	}
	
	/**
	 * Sets the entire image informations map structure.
	 * 
	 * @param imgInfo hash map containing the image informations key/value pairs
	 */
	public void setImageInfo(HashMap<HipiKeyImageInfo, Object> imgInfo) {
		this.imageInfo = new HashMap<HipiKeyImageInfo, Object>(imgInfo);
	}

	/**
	 * Adds an metadata field to this header object. The information consists of a
	 * key-value pair where the key is an application-specific field name and the 
	 * value is the corresponding information for that field.
	 * 
	 * @param key the metadata field name
	 * @param value the metadata information
	 */
	public void addMetaData(String key, String value) {
		metaData.put(key, value);
	}

	/**
	 * Sets the entire metadata map structure.
	 *
	 * @param metaData hash map containing the metadata key/value pairs
	 */
	public void setMetaData(HashMap<String, String> metaData) {
		this.metaData = new HashMap<String, String>(metaData);
	}

	/**
	 * Attempt to retrieve metadata value associated with key.
	 *
	 * @param key field name of the desired metadata record
	 * @return either the value corresponding to the key or null if the
	 * key was not found
	 */
	public String getMetaData(String key) {
		return metaData.get(key);
	}

	/**
	 * Get the entire list of all metadata that applications have
	 * associated with this image.
	 *
	 * @return a hash map containing the keys and values of the metadata
	 */
	public HashMap<String, String> getAllMetaData() {
		return new HashMap<String, String>(metaData);
	}

	/**
	 * Create a binary representation of the application-specific
	 * metadata, ready to be serialized into a HIB file.
	 *
	 * @return A byte array containing the serialized hash map
	 */
	public byte[] getMetaDataAsBytes() {
		try {
			String jsonText = JSONValue.toJSONString(metaData);
			final byte[] utf8Bytes = jsonText.getBytes("UTF-8");
			return utf8Bytes;
		} catch (java.io.UnsupportedEncodingException e) {
			System.err.println("UTF-8 encoding exception in getMetaDataAsBytes()");
			return null;
		}
	}

	/**
	 * Recreates the general metadata from serialized bytes, usually
	 * from the beginning of a HIB file.
	 *
	 * @param utf8Bytes UTF-8-encoded bytes of a JSON object
	 * representing the data
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void setMetaDataFromBytes(byte[] utf8Bytes) {
		try {
			String jsonText = new String(utf8Bytes, "UTF-8");
			JSONObject jsonObject = (JSONObject)JSONValue.parse(jsonText);
			metaData = (HashMap)jsonObject;
		} catch (java.io.UnsupportedEncodingException e) {
			System.err.println("UTF-8 encoding exception in setMetaDataAsBytes()");
		}
	}

	/**
	 * Attempt to retrieve EXIF data value for specific key.
	 *
	 * @param key field name of the desired EXIF data record
	 * @return either the value corresponding to the key or null if the
	 * key was not found
	 */
	public String getExifData(String key) {
		return exifData.get(key);
	}

	/**
	 * Get the entire map of EXIF data.
	 *
	 * @return a hash map containing the keys and values of the EXIF data
	 */
	public HashMap<String, String> getAllExifData() {
		return new HashMap<String, String>(exifData);
	}

	/**
	 * Sets the entire EXIF data map structure.
	 *
	 * @param exifData hash map containing the EXIF data key/value pairs
	 */
	public void setExifData(HashMap<String, String> exifData) {
		this.exifData = new HashMap<String, String>(exifData);
	}

	/**
	 * Sets the current object to be equal to another
	 * ImageHeader. Performs deep copy of meta data.
	 *
	 * @param header Target image header.
	 */
	public void set(HipiImageHeader header) {

		this.storageFormat = header.getStorageFormat();
		this.metaData = header.getAllMetaData();
		this.exifData = header.getAllExifData();
		this.imageInfo = header.getAllImageInfo();
	}

	/**
	 * Produce readable string representation of header.
	 * @see java.lang.Object#toString
	 */
	@Override
	public String toString() {
		String out = null;

		String metaText = JSONValue.toJSONString(metaData);

		switch (storageFormat) {
		case JPEG:
		case PNG:
			out =  String.format("ImageHeader: (%s %d) %d x %d x %d meta: %s", storageFormat.toString(),
					imageInfo.get(HipiKeyImageInfo.COLOR_SPACE), imageInfo.get(HipiKeyImageInfo.WIDTH), imageInfo.get(HipiKeyImageInfo.HEIGHT), imageInfo.get(HipiKeyImageInfo.BANDS), metaText);
			break;

		case NIFTI:
			out =  String.format("ImageHeader: (%s) %d x %d x %d x %d meta: %s", storageFormat.toString(),
					imageInfo.get(HipiKeyImageInfo.X_LENGTH), imageInfo.get(HipiKeyImageInfo.Y_LENGTH), imageInfo.get(HipiKeyImageInfo.Z_LENGTH), imageInfo.get(HipiKeyImageInfo.T_LENGTH), metaText);
			break;

		case DICOM:
			out =  String.format("ImageHeader: (%s) patient_id: %s patient_name: %s rows: %d columns: %d meta: %s", storageFormat.toString(),
					imageInfo.get(HipiKeyImageInfo.PATIENT_ID), imageInfo.get(HipiKeyImageInfo.PATIENT_NAME), imageInfo.get(HipiKeyImageInfo.ROWS), imageInfo.get(HipiKeyImageInfo.COLUMNS), metaText);
			break;

		case RDA:
			throw new RuntimeException("Support for RDA image type not yet implemented.");

		case UNDEFINED:
		default:
			throw new IllegalArgumentException("Format not specified.");
		}

		return out;
	}  

	/**
	 * Serializes the HipiImageHeader object into a simple uncompressed binary format using the
	 * {@link java.io.DataOutput} interface.
	 *
	 * @see #readFields
	 * @see org.apache.hadoop.io.WritableComparable#write
	 */
	@Override
	public void write(DataOutput out) throws IOException {

		out.writeInt(storageFormat.toInteger());

		switch (storageFormat) {

		case JPEG:
		case PNG:
			out.writeInt((int) ((HipiColorSpace)imageInfo.get(HipiKeyImageInfo.COLOR_SPACE)).toInteger());
			out.writeInt((int) imageInfo.get(HipiKeyImageInfo.WIDTH));
			out.writeInt((int) imageInfo.get(HipiKeyImageInfo.HEIGHT));
			out.writeInt((int) imageInfo.get(HipiKeyImageInfo.BANDS));
			break;

		case NIFTI:
			out.writeShort((short) imageInfo.get(HipiKeyImageInfo.X_LENGTH));
			out.writeShort((short) imageInfo.get(HipiKeyImageInfo.Y_LENGTH));
			out.writeShort((short) imageInfo.get(HipiKeyImageInfo.Z_LENGTH));
			out.writeShort((short) imageInfo.get(HipiKeyImageInfo.T_LENGTH));
			break;

		case DICOM:
			out.writeUTF((String) imageInfo.get(HipiKeyImageInfo.PATIENT_ID));
			out.writeUTF((String) imageInfo.get(HipiKeyImageInfo.PATIENT_NAME));
			out.writeInt((int) imageInfo.get(HipiKeyImageInfo.ROWS));
			out.writeInt((int) imageInfo.get(HipiKeyImageInfo.COLUMNS));
			break;

		case RDA:
			throw new RuntimeException("Support for RDA image type not yet implemented.");

		case UNDEFINED:
		default:
			throw new IllegalArgumentException("Format not specified");
		}

		byte[] metaDataBytes = getMetaDataAsBytes();
		if (metaDataBytes == null || metaDataBytes.length == 0)
			out.writeInt(0);
		else {
			out.writeInt(metaDataBytes.length);
			out.write(metaDataBytes);
		}

	}

	/**
	 * Deserializes HipiImageHeader object stored in a simple uncompressed binary format using the
	 * {@link java.io.DataInput} interface. The first twenty bytes are the image storage type,
	 * color space, width, height, and number of color bands (aka channels), all stored as ints,
	 * followed by the meta data stored as a set of key/value pairs in JSON UTF-8 format.
	 *
	 * @see org.apache.hadoop.io.WritableComparable#readFields
	 */
	@Override
	public void readFields(DataInput input) throws IOException {

		this.storageFormat = HipiImageFormat.fromInteger(input.readInt());

		switch (storageFormat) {

		case JPEG:
		case PNG:
			imageInfo.put(HipiKeyImageInfo.COLOR_SPACE, input.readInt());
			imageInfo.put(HipiKeyImageInfo.WIDTH, input.readInt());
			imageInfo.put(HipiKeyImageInfo.HEIGHT, input.readInt());
			imageInfo.put(HipiKeyImageInfo.BANDS, input.readInt());
			break;

		case NIFTI:
			imageInfo.put(HipiKeyImageInfo.X_LENGTH, input.readShort());
			imageInfo.put(HipiKeyImageInfo.Y_LENGTH, input.readShort());
			imageInfo.put(HipiKeyImageInfo.Z_LENGTH, input.readShort());
			imageInfo.put(HipiKeyImageInfo.T_LENGTH, input.readShort());
			break;

		case DICOM:
			imageInfo.put(HipiKeyImageInfo.PATIENT_ID, input.readUTF());
			imageInfo.put(HipiKeyImageInfo.PATIENT_NAME, input.readUTF());
			imageInfo.put(HipiKeyImageInfo.ROWS, input.readInt());
			imageInfo.put(HipiKeyImageInfo.COLUMNS, input.readInt());
			break;

		case RDA:
			throw new RuntimeException("Support for RDA image type not yet implemented.");

		case UNDEFINED:
		default:
			throw new IllegalArgumentException("Format not specified");
		}

		int len = input.readInt();
		if (len > 0) {
			byte[] metaDataBytes = new byte[len];
			input.readFully(metaDataBytes, 0, len);
			setMetaDataFromBytes(metaDataBytes);
		}

	}

	/**
	 * Compare method inherited from the {@link java.lang.Comparable} interface. This method is
	 * currently incomplete and uses only the storage format to determine order.
	 *
	 * @param that another {@link HipiImageHeader} to compare with the current object
	 *
	 * @return An integer result of the comparison.
	 *
	 * @see java.lang.Comparable#compareTo
	 */
	@Override
	public int compareTo(HipiImageHeader that) {

		int thisFormat = this.storageFormat.toInteger();
		int thatFormat = that.storageFormat.toInteger();

		return (thisFormat < thatFormat ? -1 : (thisFormat == thatFormat ? 0 : 1));
	}

	/**
	 * Hash method inherited from the {@link java.lang.Object} base class. This method is
	 * currently incomplete and uses only the storage format to determine this hash.
	 *
	 * @return hash code for this object
	 *
	 * @see java.lang.Object#hashCode
	 */
	@Override
	public int hashCode() {
		return this.storageFormat.toInteger();
	}

}
