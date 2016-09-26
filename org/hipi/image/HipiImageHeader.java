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
 * foramts such as JPEG and PNG.
 *
 * Note that this class implements the {@link org.apache.hadoop.io.WritableComparable} interface,
 * allowing it to be used as a key/value object in MapReduce programs.
 */
public class HipiImageHeader implements WritableComparable<HipiImageHeader> {

	/**
	 * Enumeration of the image storage formats supported in HIPI (e.g, JPEG, PNG, etc.).
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

	private static final int JPEG_PNG_DIM = 4;
	private static final int NIFTI_DIM = 4;
	private static final int RDA_DIM = 1;
	private static final int DICOM_DIM = 1;

	public static final int JPEG_PNG_INDEX_COLOR_SPACE = 0;
	public static final int JPEG_PNG_INDEX_WIDTH = 1;
	public static final int JPEG_PNG_INDEX_HEIGHT = 2;
	public static final int JPEG_PNG_INDEX_BANDS = 3;

	public static final int NIFTI_INDEX_DIM_X = 0;
	public static final int NIFTI_INDEX_DIM_Y = 1;
	public static final int NIFTI_INDEX_DIM_Z = 2;
	public static final int NIFTI_INDEX_DIM_T = 3;

	public static final int RDA_INDEX = 0;

	public static final int DICOM_INDEX = 0;

	/**
	 * Format used to store image on HDFS
	 */
	private HipiImageFormat storageFormat;

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
	 * Header's values number
	 */
	private int dim;

	/**
	 * Header's values
	 */
	private int[] values;

	/**
	 * Creates an ImageHeader default.
	 */
	public HipiImageHeader(HipiImageFormat storageFormat, int dim, byte[] metaDataBytes, Map<String,String> exifData) {

		if ((		(storageFormat == HipiImageFormat.JPEG || storageFormat == HipiImageFormat.PNG) && dim != JPEG_PNG_DIM)
				|| 	(storageFormat == HipiImageFormat.NIFTI && dim != NIFTI_DIM)
				|| 	(storageFormat == HipiImageFormat.RDA && dim != RDA_DIM))
			throw new IllegalArgumentException(String.format("Invalid storage format (%s) for this dimension (%d).", storageFormat.toString(), dim));

		this.storageFormat = storageFormat;

		if (metaDataBytes != null)
			setMetaDataFromBytes(metaDataBytes);

		this.exifData = exifData;

		this.dim = dim;
		values = new int[dim];
	}

	/**
	 * Creates an ImageHeader for Jpeg or Png.
	 */
	public HipiImageHeader(HipiImageFormat storageFormat, HipiColorSpace colorSpace, int width, int height, int bands, byte[] metaDataBytes, Map<String,String> exifData) {

		this(storageFormat, JPEG_PNG_DIM, metaDataBytes, exifData);

		if (!(storageFormat == HipiImageFormat.JPEG || storageFormat == HipiImageFormat.PNG)) 
			throw new IllegalArgumentException(String.format("Invalid storage format (%s) for this constructor, only Jpeg or Png.", storageFormat.toString()));

		if (width < 1 || height < 1 || bands < 1)
			throw new IllegalArgumentException(String.format("Invalid spatial dimensions or number of bands: (%d,%d,%d)", width, height, bands));

		values[JPEG_PNG_INDEX_COLOR_SPACE] 	= colorSpace.toInteger();
		values[JPEG_PNG_INDEX_WIDTH] 		= width;
		values[JPEG_PNG_INDEX_HEIGHT] 		= height;
		values[JPEG_PNG_INDEX_BANDS] 		= bands;

	}

	/**
	 * Creates an ImageHeader for Nifti.
	 */
	public HipiImageHeader(HipiImageFormat storageFormat, int dimX, int dimY, int dimZ, int dimT,  byte[] metaDataBytes,  Map<String,String> exifData) {

		this(storageFormat, NIFTI_DIM, metaDataBytes, exifData);

		if (storageFormat != HipiImageFormat.NIFTI)
			throw new IllegalArgumentException(String.format("Invalid storage format (%s) for this constructor, only Nifti.", storageFormat.toString()));

		if (dimX < 1 || dimY < 1 || dimZ < 1 || dimT < 0)
			throw new IllegalArgumentException(String.format("Invalid spatial dimensions: (%d,%d,%d,%d)", dimX, dimY, dimZ, dimT));

		values[NIFTI_INDEX_DIM_X] = dimX;
		values[NIFTI_INDEX_DIM_Y] = dimY;
		values[NIFTI_INDEX_DIM_Z] = dimZ;
		values[NIFTI_INDEX_DIM_T] = dimT;

	}

	/**
	 * Creates an ImageHeader for Dicom.
	 */
	public HipiImageHeader(HipiImageFormat storageFormat, byte[] metaDataBytes,  Map<String,String> exifData) {

		this.storageFormat = storageFormat;

		if (metaDataBytes != null)
			setMetaDataFromBytes(metaDataBytes);

		this.exifData = exifData;		

		if (storageFormat != HipiImageFormat.DICOM)
			throw new IllegalArgumentException(String.format("Invalid storage format (%s) for this constructor, only DICOM.", storageFormat.toString()));

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
	 * Adds an metadata field to this header object. The information consists of a
	 * key-value pair where the key is an application-specific field name and the 
	 * value is the corresponding information for that field.
	 * 
	 * @param key
	 *            the metadata field name
	 * @param value
	 *            the metadata information
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
	 * @return a hash map containing the keys and values of the metadata
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
	 * Get the number of header's values.
	 *
	 * @return Number of header's values.
	 */
	public int getDimension() {
		return dim;
	}

	/**
	 * Get header's values array.
	 *
	 * @return Header's values array.
	 */
	public int[] getValues() {
		return values;
	}

	/**
	 * Get header's value from index.
	 *
	 * @return Header's value from index.
	 */
	public int getValue(int i) {
		return values[i];
	}

	/**
	 * Sets value.
	 *
	 * @param index of values array
	 * 
	 * @param new value
	 */
	public void setValue(int index, int value) {
		this.values[index] = value;
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

		switch (storageFormat) {
		case JPEG:
		case PNG:
			if (header.getDimension() != JPEG_PNG_DIM)
				throw new IllegalArgumentException(String.format("Invalid header's values number (%d) for Jpeg or Png", header.getDimension()));
			this.dim = JPEG_PNG_DIM;
			break;

		case NIFTI:
			if (header.getDimension() != NIFTI_DIM) 
				throw new IllegalArgumentException(String.format("Invalid header's values number (%d) for Nifti", header.getDimension()));
			this.dim = NIFTI_DIM;
			break;

		case RDA:
			throw new RuntimeException("Support for RDA image type not yet implemented.");

		case UNDEFINED:
		default:
			throw new IllegalArgumentException("Format not specified.");
		}

		this.values = header.getValues().clone();

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
			out =  String.format("ImageHeader: (%d %d) %d x %d x %d meta: %s", storageFormat.toInteger(), values[JPEG_PNG_INDEX_COLOR_SPACE], values[JPEG_PNG_INDEX_WIDTH], values[JPEG_PNG_INDEX_HEIGHT], values[JPEG_PNG_INDEX_BANDS], metaText);
			break;

		case NIFTI:
			out =  String.format("ImageHeader: (%d) %d x %d x %d x %d meta: %s", storageFormat.toInteger(), values[NIFTI_INDEX_DIM_X], values[NIFTI_INDEX_DIM_Y], values[NIFTI_INDEX_DIM_Z], values[NIFTI_INDEX_DIM_T], metaText);
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
			out.writeInt(values[JPEG_PNG_INDEX_COLOR_SPACE]);
			out.writeInt(values[JPEG_PNG_INDEX_WIDTH]);
			out.writeInt(values[JPEG_PNG_INDEX_HEIGHT]);
			out.writeInt(values[JPEG_PNG_INDEX_BANDS]);
			break;

		case NIFTI:
			out.writeInt(values[NIFTI_INDEX_DIM_X]);
			out.writeInt(values[NIFTI_INDEX_DIM_Y]);
			out.writeInt(values[NIFTI_INDEX_DIM_Z]);
			out.writeInt(values[NIFTI_INDEX_DIM_T]);
			break;

		case DICOM:
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
			values = new int[JPEG_PNG_DIM];

			values[JPEG_PNG_INDEX_COLOR_SPACE] = input.readInt();
			values[JPEG_PNG_INDEX_WIDTH] = input.readInt();
			values[JPEG_PNG_INDEX_HEIGHT] = input.readInt();
			values[JPEG_PNG_INDEX_BANDS] = input.readInt();
			break;

		case NIFTI:
			values = new int[NIFTI_DIM];

			values[NIFTI_INDEX_DIM_X] = input.readInt();
			values[NIFTI_INDEX_DIM_Y] = input.readInt();
			values[NIFTI_INDEX_DIM_Z] = input.readInt();
			values[NIFTI_INDEX_DIM_T] = input.readInt();
			break;

		case DICOM:
			values = new int[DICOM_DIM];
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
