package org.biohipi.image;

import org.biohipi.image.BioHipiImageHeader;
import org.biohipi.image.BioHipiImageHeader.BioHipiImageFormat;
import org.biohipi.image.BioHipiImageHeader.BioHipiKeyMetaData;
import org.apache.hadoop.io.Writable;

import java.util.HashMap;


/**
 * An abstract base class from which all concrete image classes in BioHIPI must be derived. This class
 * implements the {@link org.apache.hadoop.io.Writable} interface so that it can be used as a value
 * object in a MapReduce program.
 */
public abstract class BioHipiImage implements Writable {

	/**
	 * Enumeration of the supported image object types in BioHIPI (FloatImage, ByteImage, NiftiImage, DicomImage).
	 */
	public enum BioHipiImageType {
		UNDEFINED(0x0), RASTER(0x1), NIFTI(0x2), DICOM(0x3);

		private int type;

		/**
		 * Creates a BioHipiImageType from an int.
		 *
		 * @param format Integer representation of BioHipiImageType.
		 */
		BioHipiImageType(int type) {
			this.type = type;
		}

		/**
		 * Creates a BioHipiImageType from an int.
		 *
		 * @param type integer representation of BioHipiImageType
		 *
		 * @return Associated BioHipiImageType.
		 *
		 * @throws IllegalArgumentException if the parameter does not correspond to a valid
		 * BioHipiImageType.
		 */
		public static BioHipiImageType fromInteger(int type) throws IllegalArgumentException {
			for (BioHipiImageType typ : values()) {
				if (typ.type == type) {
					return typ;
				}
			}
			throw new IllegalArgumentException(String.format("There is no HipiImageType enum value " +
					"associated with integer [%d]", type));
		}

		/** 
		 * @return Integer representation of BioHipiImageType
		 */
		public int toInteger() {
			return type;
		}

		/**
		 * Default BioHipiImageType.
		 *
		 * @return BioHipiImageType.UNDEFINED
		 */
		public static BioHipiImageType getDefault() {
			return UNDEFINED;
		}

	}

	/**
	 * Every BioHipiImage contains a BioHipiImageHeader that stores universally available information about
	 * the image such as its spatial dimensions and patient id.
	 */
	protected BioHipiImageHeader header;

	/**
	 * Default constructor. Sets header field to null.
	 */
	protected BioHipiImage() {
		this.header = null;
	}

	/**
	 * Set value of header field.
	 *
	 * @param header header object to use as source of assignment
	 *
	 * @throws IllegalArgumentException of the provided header is null or contains invalid values
	 */
	public void setHeader(BioHipiImageHeader header) throws IllegalArgumentException {
		if (header == null)
			throw new IllegalArgumentException("Image header must not be null.");
		this.header = header;
	}

	/**
	 * Get image type identifier.
	 *
	 * @return BioHipiImageType.UNDEFINED
	 */
	public BioHipiImageType getType() {
		return BioHipiImageType.UNDEFINED;
	}

	/**
	 * Get storage format of image.
	 *
	 * @return storage format of image
	 */
	public BioHipiImageFormat getStorageFormat() {
		return header.getStorageFormat();
	}

	/**
	 * Get meta data value for particular key.
	 * 
	 * @param key of meta data, you may use {@link BioHipiKeyMetaData}
	 * @return meta data value as String (null if key does not exist in meta data dictionary)
	 */
	public String getMetaData(String key) {
		return header.getMetaData(key);
	}

	/**
	 * Get the entire image meta data dictionary as a {@link HashMap}.
	 *
	 * @return a hash map containing the image meta data key/value pairs
	 */
	public HashMap<String, String> getAllMetaData() {
		return header.getAllMetaData();
	}


	/**
	 * Get {@link #getBioHipiImageHeader()} of image.
	 *
	 * @return {@link #getBioHipiImageHeader()} of image.
	 */
	public BioHipiImageHeader getBioHipiImageHeader() {
		return this.header;
	}

}
