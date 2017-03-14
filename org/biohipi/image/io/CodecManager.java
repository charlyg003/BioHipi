package org.biohipi.image.io;

import org.biohipi.image.BioHipiImage;
import org.biohipi.image.BioHipiImageHeader.BioHipiImageFormat;
import org.biohipi.image.io.JpegCodec;
import org.biohipi.image.io.PngCodec;

/**
 * Finds a suitable {@link ImageEncoder} or {@link ImageDecoder} for a specific
 * {@link BioHipiImageFormat}.
 */
public final class CodecManager {

	/**
	 * Find a {@link ImageDecoder} capable of deserializing a {@link BioHipiImage}
	 * object stored in a specific {@link BioHipiImageFormat}.
	 *
	 * @param format
	 *            storage format to assume during deserialization
	 *
	 * @return image decoder object
	 *
	 * @throws IllegalArgumentException
	 *             if format is invalid or currently unsupported
	 */
	static public ImageDecoder getDecoder(BioHipiImageFormat format) throws IllegalArgumentException {
		switch (format) {
		case JPEG:
			return JpegCodec.getInstance();
		case PNG:
			return PngCodec.getInstance();
		case NIFTI:
			return NiftiCodec.getInstance();
		case DICOM:
			return DicomCodec.getInstance();
		default:
			throw new IllegalArgumentException("Image format currently unsupported.");
		}
	}

	/**
	 * Find a {@link ImageEncoder} capable of serializing a {@link BioHipiImage} to
	 * a target {@link BioHipiImageFormat}.
	 *
	 * @param format
	 *            storage format to target during serialization
	 *
	 * @return image encoder object
	 *
	 * @throws IllegalArgumentException
	 *             if format is invalid or currently unsupported
	 */
	static public ImageEncoder getEncoder(BioHipiImageFormat format) throws IllegalArgumentException {
		switch (format) {
		case JPEG:
			return JpegCodec.getInstance();
		case PNG:
			return PngCodec.getInstance();
		case NIFTI:
			return NiftiCodec.getInstance();
		case DICOM:
			return DicomCodec.getInstance();
		default:
			throw new IllegalArgumentException("Image format currently unsupported.");
		}
	}

}
