package org.hipi.image.io;

import org.hipi.image.HipiImageHeader;
import org.hipi.image.NiftiImage;
import org.hipi.image.HipiImageHeader.HipiImageFormat;
import org.hipi.niftijio.NiftiHeader;
import org.hipi.niftijio.NiftiVolume;
import org.hipi.image.HipiImage;
import org.hipi.image.HipiImageFactory;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Abstract base class for objects that serve as both an {@link ImageDecoder}
 * and {@link ImageEncoder} for a particular storage format (e.g., JPEG, PNG,
 * etc.).
 */
public class NiftiCodec implements ImageDecoder, ImageEncoder {

	private static final NiftiCodec staticObject = new NiftiCodec();

	public static NiftiCodec getInstance() {
		return staticObject;
	}

	@Override
	public void encodeImage(HipiImage image, OutputStream outputStream) throws IllegalArgumentException, IOException {
		NiftiVolume niiVol = ((NiftiImage) image).getNiftiVolume();
		niiVol.write(outputStream);
	}

	@Override
	public HipiImageHeader decodeHeader(InputStream inputStream, boolean includeExifData) throws IOException {
		NiftiHeader niiHd = new NiftiHeader();
		niiHd = NiftiHeader.readFromStream(inputStream);
		return new HipiImageHeader(HipiImageFormat.NIFTI, niiHd.dim[1], niiHd.dim[2], niiHd.dim[3],niiHd.dim[4], null, null);
	}

	@Override
	public HipiImageHeader decodeHeader(InputStream inputStream) throws IOException {
		return decodeHeader(inputStream, false);
	}

	@Override
	public HipiImage decodeImage(InputStream inputStream, HipiImageHeader imageHeader, HipiImageFactory imageFactory,
			boolean includeExifData) throws IllegalArgumentException, IOException {
		
		return new NiftiImage(inputStream, imageHeader);
	}

	@Override
	public HipiImage decodeHeaderAndImage(InputStream inputStream, HipiImageFactory imageFactory,
			boolean includeExifData) throws IOException, IllegalArgumentException {
		
		BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);
		bufferedInputStream.mark(Integer.MAX_VALUE);
		HipiImageHeader header = decodeHeader(bufferedInputStream, includeExifData);
		
		bufferedInputStream.reset();
		return decodeImage(bufferedInputStream, header, imageFactory, false);
		
	}

}
