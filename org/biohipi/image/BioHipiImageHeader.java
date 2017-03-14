package org.biohipi.image;

import org.apache.hadoop.io.WritableComparable;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * The header information for a BioHipiImage. BioHipiImageHeader encapsulates universally available
 * information about a biomedical image such as width, height, storage format, color space, patient id, patient name, etc.
 * <br>
 * The {@link org.biohipi.image.io} package provides classes for reading (decoding) BioHipiImageHeader
 * from both {@link org.biohipi.imagebundle.BioHipiImageBundle} files and various standard image storage
 * formats such as JPEG, PNG, NIfTI and DICOM.
 * <br>
 * Note that this class implements the {@link org.apache.hadoop.io.WritableComparable} interface,
 * allowing it to be used as a key/value object in MapReduce programs.
 */
public class BioHipiImageHeader implements WritableComparable<BioHipiImageHeader> {

	/**
	 * Enumeration of the image storage formats supported in BioHIPI (JPEG, PNG, NIFTI, DICOM).
	 */
	public enum BioHipiImageFormat {
		UNDEFINED(0x0), JPEG(0x1), PNG(0x2), NIFTI(0x3), DICOM(0x4);

		private int format;

		/**
		 * Creates an ImageFormat from an int.
		 *
		 * @param format Integer representation of ImageFormat.
		 */
		BioHipiImageFormat(int format) {
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
		public static BioHipiImageFormat fromInteger(int format) throws IllegalArgumentException {
			for (BioHipiImageFormat fmt : values()) {
				if (fmt.format == format) {
					return fmt;
				}
			}
			throw new IllegalArgumentException(String.format("There is no BioHipiImageFormat enum value " +
					"associated with integer [%d]", format));
		}

		/** 
		 * @return Integer representation of ImageFormat.
		 */
		public int toInteger() {
			return format;
		}

		/**
		 * Default BioHipiImageFormat.
		 *
		 * @return BioHipiImageFormat.UNDEFINED
		 */
		public static BioHipiImageFormat getDefault() {
			return UNDEFINED;
		}

	}

	/**
	 * Enumeration of the color spaces supported in BioHIPI.
	 */
	public enum BioHipiColorSpace {
		UNDEFINED(0x0), RGB(0x1), LUM(0x2);

		private int cspace;

		/**
		 * Creates a BioHipiColorSpace from an int
		 *
		 * @param format Integer representation of ColorSpace.
		 */
		BioHipiColorSpace(int cspace) {
			this.cspace = cspace;
		}

		/**
		 * Creates a BioHipiColorSpace from an int.
		 *
		 * @param cspace Integer representation of ColorSpace.
		 *
		 * @return Associated BioHipiColorSpace value.
		 *
		 * @throws IllegalArgumentException if parameter does not correspond to a valid BioHipiColorSpace.
		 */
		public static BioHipiColorSpace fromInteger(int cspace) throws IllegalArgumentException {
			for (BioHipiColorSpace cs : values()) {
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
		public static BioHipiColorSpace getDefault() {
			return RGB;
		}

	}


	/**
	 * BioHipiKeyMetaData provides some Strings to use as a key 
	 * in {@link BioHipiImageHeader#metaData} 
	 */
	public static class BioHipiKeyMetaData {
		
		/** source file */
		public static final String SOURCE = "source";

		/** {@link BioHipiColorSpace} of the Jpeg or Png Images */
		public static final String COLOR_SPACE = "color space";

		/** width of the Jpeg or Png Images */
		public static final String WIDTH = "width";

		/** height of the Jpeg or Png Images */
		public static final String HEIGHT = "height";

		/** number of Bands of the Jpeg or Png Images */
		public static final String BANDS = "number bands";

		/** x-axis length of the NIfTI images */
		public static final String X_LENGTH = "x-axis";

		/** y-axis length of the NIfTI images */
		public static final String Y_LENGTH = "y-axis";

		/** z-axis length of the NIfTI images */
		public static final String Z_LENGTH = "z-axis";

		/** t-axis length of the NIfTI images */
		public static final String T_LENGTH = "t-axis";

		/** patient id of the DICOM images */
		public static final String PATIENT_ID = "patient id";

		/** patient name of the DICOM images */
		public static final String PATIENT_NAME = "patient name";

		/** rows of the DICOM images */
		public static final String ROWS = "rows";

		/** columns of the DICOM images */
		public static final String COLUMNS = "columns";
	}

	/**
	 * Format used to store image on HDFS
	 */
	private BioHipiImageFormat storageFormat;

	/**
	 * A map containing key/value pairs of meta data associated with the
	 * image. For example, this would be the correct place to store the image 
	 * tile offset and size if you were using a HIB to store a very large
	 * image as a collection of smaller image tiles. Another example
	 * would be using this dictionary to store the source url for an
	 * image downloaded from the Internet.
	 */
	private Map<String, String> metaData = new HashMap<String,String>();
	
	/**
	 * Creates an ImageHeader default.
	 */
	public BioHipiImageHeader(BioHipiImageFormat storageFormat) {
		this.storageFormat = storageFormat;
	}

	/**
	 * Creates an ImageHeader by calling {@link #readFields(DataInput)} on the data input
	 * object.
	 */
	public BioHipiImageHeader(DataInput input) throws IOException {
		readFields(input);
	}

	/**
	 * Get the image storage type.
	 *
	 * @return Current image storage type.
	 */
	public BioHipiImageFormat getStorageFormat() {
		return storageFormat;
	}

	/**
	 * Join other metadata to the existing ones 
	 * in this header
	 * 
	 * @param metaData to join
	 */
	public void appendMetaData(Map<String, String> metaData) {
		this.metaData.putAll(metaData);
	}

	/**
	 * Adds an metadata field to this header object. The information consists of a
	 * key-value pair where the key is an application-specific field name and the 
	 * value is the corresponding information for that field.
	 * 
	 * @param key the metadata field name, you may use {@link BioHipiKeyMetaData}
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
	 * @param key field name of the desired metadata record,
	 * you may use {@link BioHipiKeyMetaData}
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
	 * Create a String representation of the application-specific
	 * metadata.
	 *
	 * @return A String containing the metadata information
	 */
	public String getMetaDataAsString() {
		return JSONValue.toJSONString(metaData);
	}

	/**
	 * Create a binary representation of the application-specific
	 * meta data, ready to be serialized into a HIB file.
	 *
	 * @return A byte array containing the serialized hash map
	 */
	public byte[] getMetaDataAsBytes() {
		try {
			String jsonText = getMetaDataAsString();
			final byte[] utf8Bytes = jsonText.getBytes("UTF-8");
			return utf8Bytes;
		} catch (java.io.UnsupportedEncodingException e) {
			System.err.println("UTF-8 encoding exception in getMetaDataAsBytes()");
			return null;
		}
	}

	/**
	 * Recreates the general meta data from serialized bytes, usually
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
	 * Sets the current object to be equal to another
	 * ImageHeader. Performs deep copy of meta data.
	 *
	 * @param header Target image header.
	 */
	public void set(BioHipiImageHeader header) {
		this.storageFormat = header.getStorageFormat();
		this.metaData = header.getAllMetaData();
	}

	/**
	 * Produce readable string representation of header.
	 * @see java.lang.Object#toString
	 */
	@Override
	public String toString() {
		StringBuilder out = null;

		switch (storageFormat) {
		case JPEG:
		case PNG:
			out = new StringBuilder(String.format("ImageHeader: (%s %s) %s x %s x %s\n", storageFormat.toString(), getMetaData(BioHipiKeyMetaData.COLOR_SPACE), getMetaData(BioHipiKeyMetaData.WIDTH), getMetaData(BioHipiKeyMetaData.HEIGHT), getMetaData(BioHipiKeyMetaData.BANDS)));
			for (Map.Entry<String, String> entry : metaData.entrySet()) {
				String key = entry.getKey();
				if (key == BioHipiKeyMetaData.COLOR_SPACE || key == BioHipiKeyMetaData.WIDTH || key == BioHipiKeyMetaData.HEIGHT || key == BioHipiKeyMetaData.BANDS)
					continue;
				String value = entry.getValue();
				out.append(String.format("%s: %s\n", key, value));
			}
			break;

		case NIFTI:
			out = new StringBuilder(String.format("ImageHeader: (%s) %s x %s x %s x %s\n", storageFormat.toString(), getMetaData(BioHipiKeyMetaData.X_LENGTH), getMetaData(BioHipiKeyMetaData.Y_LENGTH), getMetaData(BioHipiKeyMetaData.Z_LENGTH), getMetaData(BioHipiKeyMetaData.T_LENGTH)));
			for (Map.Entry<String, String> entry : metaData.entrySet()) {
				String key = entry.getKey();
				if (key == BioHipiKeyMetaData.X_LENGTH || key == BioHipiKeyMetaData.Y_LENGTH || key == BioHipiKeyMetaData.Z_LENGTH || key == BioHipiKeyMetaData.T_LENGTH)
					continue;
				String value = entry.getValue();
				out.append(String.format("%s: %s\n", key, value));
			}
			break;

		case DICOM:
			out =  new StringBuilder(String.format("ImageHeader: (%s) patient_id: %s patient_name: %s rows: %s columns: %s\n", storageFormat.toString(), getMetaData(BioHipiKeyMetaData.PATIENT_ID), getMetaData(BioHipiKeyMetaData.PATIENT_NAME), getMetaData(BioHipiKeyMetaData.ROWS), getMetaData(BioHipiKeyMetaData.COLUMNS)));
			for (Map.Entry<String, String> entry : metaData.entrySet()) {
				String key = entry.getKey();
				if (key == BioHipiKeyMetaData.PATIENT_ID || key == BioHipiKeyMetaData.PATIENT_NAME || key == BioHipiKeyMetaData.ROWS || key == BioHipiKeyMetaData.COLUMNS)
					continue;
				String value = entry.getValue();
				out.append(String.format("%s: %s\n", key, value));
			}
			break;

		case UNDEFINED:
		default:
			throw new IllegalArgumentException("Format not specified.");
		}
		

		return new String(out);
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

		this.storageFormat = BioHipiImageFormat.fromInteger(input.readInt());

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
	 * @param that another {@link BioHipiImageHeader} to compare with the current object
	 *
	 * @return An integer result of the comparison.
	 *
	 * @see java.lang.Comparable#compareTo
	 */
	@Override
	public int compareTo(BioHipiImageHeader that) {

		int thisFormat = this.storageFormat.toInteger();
		int thatFormat = that.storageFormat.toInteger();

		return (thisFormat < thatFormat ? -1 : (thisFormat == thatFormat ? 0 : 1));
	}

}
