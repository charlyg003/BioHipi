package org.hipi.image.io;

import org.hipi.image.HipiImageHeader;
import org.hipi.image.NiftiImage;
import org.hipi.image.HipiImageHeader.HipiImageFormat;
import org.hipi.image.HipiImageHeader.HipiKeyImageInfo;
import org.hipi.util.niftijio.NiftiHeader;
import org.hipi.util.niftijio.NiftiVolume;
import org.hipi.image.HipiImage;
import org.hipi.image.HipiImageFactory;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * Class for objects that serve as both an {@link ImageDecoder}
 * and {@link ImageEncoder} for the Nifti image storage format.
 */
public class NiftiCodec implements ImageDecoder, ImageEncoder {

	private static final NiftiCodec staticObject = new NiftiCodec();

	public static NiftiCodec getInstance() {
		return staticObject;
	}
	
	@Override
	public HipiImageHeader decodeHeader(InputStream inputStream) throws IOException {
		return decodeHeader(inputStream, false);
	}
	
	@Override
	public HipiImageHeader decodeHeader(InputStream inputStream, boolean includeExifData) throws IOException {
		NiftiHeader niiHd = new NiftiHeader();
		niiHd = NiftiHeader.read(inputStream);
		
		Map<HipiKeyImageInfo, Object> values = new HashMap<HipiKeyImageInfo, Object>();
		values.put(HipiKeyImageInfo.X_LENGTH, niiHd.dim[1]);
		values.put(HipiKeyImageInfo.Y_LENGTH, niiHd.dim[2]);
		values.put(HipiKeyImageInfo.Z_LENGTH, niiHd.dim[3]);
		values.put(HipiKeyImageInfo.T_LENGTH, niiHd.dim[4]);
		
		return new HipiImageHeader(HipiImageFormat.NIFTI, values, null, null);
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

	/**
	 * @return image represented as a {@link NiftiImage}
	 */
	@Override
	public HipiImage decodeImage(InputStream inputStream, HipiImageHeader imageHeader, HipiImageFactory imageFactory,
			boolean includeExifData) throws IllegalArgumentException, IOException {
		
		return new NiftiImage(inputStream, imageHeader);
	}
	
	@Override
	public void encodeImage(HipiImage image, OutputStream outputStream) throws IllegalArgumentException, IOException {
		NiftiVolume niiVol = ((NiftiImage) image).getNiftiVolume();
		niiVol.write(outputStream);
	}
}