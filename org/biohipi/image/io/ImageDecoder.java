package org.biohipi.image.io;

import org.biohipi.image.BioHipiImageHeader;
import org.biohipi.image.BioHipiImage;

import java.io.IOException;
import java.io.InputStream;

/**
 * Interface for decoding a {@link BioHipiImageHeader} and {@link BioHipiImage} from a
 * Java {@link java.io.InputStream}.
 */
public interface ImageDecoder {

	/**
	 * Read and decode header for image accessed through a Java
	 * {@link java.io.InputStream}.
	 *
	 * @param inputStream
	 *            input stream containing serialized image data
	 *
	 * @return image header data represented as a {@link BioHipiImageHeader}
	 *
	 * @throws IOException
	 *             if an error is encountered while reading from input stream
	 */
	public BioHipiImageHeader decodeHeader(InputStream inputStream) throws IOException;

	/**
	 * Read and decode image from a Java {@link java.io.InputStream}.
	 *
	 * @param inputStream
	 *            input stream containing serialized image data
	 * @param imageHeader
	 *            image header that was previously initialized
	 * @param imageFactory
	 *            factory object capable of creating objects of desired
	 *            HipiImage type
	 *
	 * @return image represented as a {@link BioHipiImage}
	 *
	 * @throws IllegalArgumentException
	 *             if parameters are invalid or do not agree with image data
	 * @throws IOException
	 *             if an error is encountered while reading from the input
	 *             stream
	 */
	public BioHipiImage decodeImage(InputStream inputStream, BioHipiImageHeader imageHeader) throws IllegalArgumentException, IOException;

	/**
	 * Read and decode both image header and image pixel data from a Java
	 * {@link java.io.InputStream}. Both of these decoded objects can be
	 * accessed through the {@link BioHipiImage} object returned by this method.
	 *
	 * @param inputStream
	 *            input stream containing serialized image data
	 * @param imageFactory
	 *            factory object capable of creating objects of desired
	 *            HipiImage type
	 *
	 * @return image represented as a {@link BioHipiImage}
	 *
	 * @throws IllegalArgumentException
	 *             if parameters are invalid
	 * @throws IOException
	 *             if an error is encountered while reading from the input
	 *             stream
	 */
	public BioHipiImage decodeHeaderAndImage(InputStream inputStream) throws IOException, IllegalArgumentException;

}
