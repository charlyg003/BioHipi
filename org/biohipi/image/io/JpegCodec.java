package org.biohipi.image.io;

import org.biohipi.image.BioHipiImageHeader;
import org.biohipi.image.BioHipiImageHeader.BioHipiImageFormat;
import org.biohipi.image.BioHipiImageHeader.BioHipiKeyMetaData;
import org.biohipi.image.BioHipiImageHeader.BioHipiColorSpace;
import org.biohipi.image.BioHipiImage;
import org.biohipi.image.RasterImage;

import java.awt.image.BufferedImage;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;

import javax.imageio.IIOImage;
import javax.imageio.ImageIO;
import javax.imageio.ImageWriteParam;
import javax.imageio.ImageWriter;
import javax.imageio.stream.ImageOutputStream;

/**
 * Extends {@link RasterCodec} and serves as both an {@link ImageDecoder} and
 * {@link ImageEncoder} for the JPEG image storage format.
 */
public class JpegCodec extends RasterCodec {

	private static final JpegCodec staticObject = new JpegCodec();

	public static JpegCodec getInstance() {
		return staticObject;
	}

	public BioHipiImageHeader decodeHeader(InputStream inputStream)
			throws IOException, IllegalArgumentException {

		DataInputStream dis = new DataInputStream(new BufferedInputStream(inputStream));
		dis.mark(Integer.MAX_VALUE);

		// all JPEGs start with -40
		short magic = dis.readShort();
		if (magic != -40)
			return null;

		int width = 0, height = 0, depth = 0;

		byte[] data = new byte[6];

		// read in each block to determine resolution and bit depth
		for (;;) {
			dis.read(data, 0, 4);
			if ((data[0] & 0xff) != 0xff)
				return null;
			if ((data[1] & 0xff) == 0x01 || ((data[1] & 0xff) >= 0xd0 && (data[1] & 0xff) <= 0xd7))
				continue;
			long length = (((data[2] & 0xff) << 8) | (data[3] & 0xff)) - 2;
			if ((data[1] & 0xff) == 0xc0 || (data[1] & 0xff) == 0xc2) {
				dis.read(data);
				height = ((data[1] & 0xff) << 8) | (data[2] & 0xff);
				width = ((data[3] & 0xff) << 8) | (data[4] & 0xff);
				depth = data[0] & 0xff;
				break;
			} else {
				while (length > 0) {
					long skipped = dis.skip(length);
					if (skipped == 0)
						break;
					length -= skipped;
				}
			}
		}

		if (depth != 8) {
			throw new IllegalArgumentException(String.format("Image has unsupported bit depth [%d].", depth));
		}

		BioHipiImageHeader header = new BioHipiImageHeader(BioHipiImageFormat.JPEG);
		header.addMetaData(BioHipiKeyMetaData.COLOR_SPACE, BioHipiColorSpace.RGB.toString());
		header.addMetaData(BioHipiKeyMetaData.WIDTH, String.valueOf(width));
		header.addMetaData(BioHipiKeyMetaData.HEIGHT, String.valueOf(height));
		header.addMetaData(BioHipiKeyMetaData.BANDS, "3");

		return header;
	}

	public void encodeImage(BioHipiImage image, OutputStream outputStream) throws IllegalArgumentException, IOException {

		if (!(RasterImage.class.isAssignableFrom(image.getClass()))) {
			throw new IllegalArgumentException("JPEG encoder supports only RasterImage input types.");
		}

		if (((RasterImage) image).getWidth() <= 0 || ((RasterImage) image).getHeight() <= 0) {
			throw new IllegalArgumentException("Invalid image resolution.");
		}
		if (((RasterImage) image).getColorSpace() != BioHipiColorSpace.RGB) {
			throw new IllegalArgumentException("JPEG encoder supports only RGB color space.");
		}
		if (((RasterImage) image).getNumBands() != 3) {
			throw new IllegalArgumentException("JPEG encoder supports only three band images.");
		}

		// Find suitable JPEG writer in javax.imageio.ImageReader
		ImageOutputStream ios = ImageIO.createImageOutputStream(outputStream);
		Iterator<ImageWriter> writers = ImageIO.getImageWritersByFormatName("jpeg");
		ImageWriter writer = writers.next();
		writer.setOutput(ios);

		ImageWriteParam param = writer.getDefaultWriteParam();
		param.setCompressionMode(ImageWriteParam.MODE_EXPLICIT);
		param.setCompressionQuality(0.95F); // highest JPEG quality = 1.0F

		int w = ((RasterImage) image).getWidth();
		int h = ((RasterImage) image).getHeight();

		BufferedImage bufferedImage = new BufferedImage(w, h, BufferedImage.TYPE_INT_RGB);

		int[] rgb = new int[w * h];
		for (int i = 0; i < w * h; i++) {

			int r = ((RasterImage) image).getElemNonLinSRGB(i * 3 + 0);
			int g = ((RasterImage) image).getElemNonLinSRGB(i * 3 + 1);
			int b = ((RasterImage) image).getElemNonLinSRGB(i * 3 + 2);

			rgb[i] = (r << 16) | (g << 8) | b;
		}
		bufferedImage.setRGB(0, 0, w, h, rgb, 0, w);
		IIOImage iioImage = new IIOImage(bufferedImage, null, null);
		writer.write(null, iioImage, param);
	}

}
