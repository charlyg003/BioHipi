package org.hipi.image.io;

import org.hipi.image.HipiImageHeader;
import org.hipi.image.HipiImage;
import org.hipi.image.HipiImageFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;


/**
 * Class for objects that serve as both an {@link ImageDecoder}
 * and {@link ImageEncoder} for the RDA image storage format.
 * <br><i>Not yet implemented</i>.
 */
public class RdaCodec implements ImageDecoder, ImageEncoder {

	private static final RdaCodec staticObject = new RdaCodec();

	public static RdaCodec getInstance() {
		return staticObject;
	}
	
	@Override
	public HipiImageHeader decodeHeader(InputStream inputStream) throws IOException {
		// TODO
		return null;
	}

	@Override
	public HipiImageHeader decodeHeader(InputStream inputStream, boolean includeExifData) throws IOException {
		// TODO
		return null;
	}

	@Override
	public HipiImage decodeImage(InputStream inputStream, HipiImageHeader imageHeader, HipiImageFactory imageFactory,
			boolean includeExifData) throws IllegalArgumentException, IOException {
		// TODO
		return null;
	}

	@Override
	public HipiImage decodeHeaderAndImage(InputStream inputStream, HipiImageFactory imageFactory,
			boolean includeExifData) throws IOException, IllegalArgumentException {
		// TODO
		return null;
	}

	@Override
	public void encodeImage(HipiImage image, OutputStream outputStream) throws IllegalArgumentException, IOException {
		// TODO
	}
	
}
