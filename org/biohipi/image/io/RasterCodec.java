package org.biohipi.image.io;

import org.biohipi.image.BioHipiImageHeader;
import org.biohipi.image.BioHipiImage;
import org.biohipi.image.RasterImage;

import java.awt.image.BufferedImage;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

import javax.imageio.ImageIO;

/**
 * Abstract base class for objects that serve as both an {@link ImageDecoder}
 * and {@link ImageEncoder} for a particular storage format (e.g., JPEG, PNG).
 */
public abstract class RasterCodec implements ImageDecoder, ImageEncoder {

	public BioHipiImage decodeHeaderAndImage(InputStream inputStream) throws IOException, IllegalArgumentException {
		BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);
		bufferedInputStream.mark(Integer.MAX_VALUE);
		BioHipiImageHeader header = decodeHeader(bufferedInputStream);
		bufferedInputStream.reset();
		return decodeImage(bufferedInputStream, header);
	}

	/**
	 * Default image decode method that uses the available ImageIO plugins.
	 *
	 * @see ImageDecoder#decodeImage
	 */
	public BioHipiImage decodeImage(InputStream inputStream, BioHipiImageHeader imageHeader) throws IllegalArgumentException, IOException {

		DataInputStream dis = new DataInputStream(new BufferedInputStream(inputStream));
		dis.mark(Integer.MAX_VALUE);

		// Find suitable ImageIO plugin (should be TwelveMonkeys)
		BufferedImage javaImage = ImageIO.read(dis);// inputStream);

		return new RasterImage(imageHeader, javaImage);
	}

}
