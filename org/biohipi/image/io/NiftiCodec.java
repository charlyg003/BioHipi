package org.biohipi.image.io;

import org.biohipi.image.BioHipiImageHeader;
import org.biohipi.image.NiftiImage;
import org.biohipi.image.BioHipiImageHeader.BioHipiImageFormat;
import org.biohipi.image.BioHipiImageHeader.BioHipiKeyMetaData;
import org.biohipi.util.niftijio.NiftiHeader;
import org.biohipi.util.niftijio.NiftiVolume;
import org.biohipi.image.BioHipiImage;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

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
	public BioHipiImageHeader decodeHeader(InputStream inputStream) throws IOException {
		NiftiHeader niiHd = new NiftiHeader();
		niiHd = NiftiHeader.read(inputStream);
		
		BioHipiImageHeader header = new BioHipiImageHeader(BioHipiImageFormat.NIFTI);
		header.addMetaData(BioHipiKeyMetaData.X_LENGTH, String.valueOf(niiHd.dim[1]));
		header.addMetaData(BioHipiKeyMetaData.Y_LENGTH, String.valueOf(niiHd.dim[2]));
		header.addMetaData(BioHipiKeyMetaData.Z_LENGTH, String.valueOf(niiHd.dim[3]));
		header.addMetaData(BioHipiKeyMetaData.T_LENGTH, String.valueOf(niiHd.dim[4]));
		
		return header;
	}
	
	@Override
	public BioHipiImage decodeHeaderAndImage(InputStream inputStream) throws IOException, IllegalArgumentException {
		
		BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);
		bufferedInputStream.mark(Integer.MAX_VALUE);
		BioHipiImageHeader header = decodeHeader(bufferedInputStream);
		
		bufferedInputStream.reset();
		return decodeImage(bufferedInputStream, header);
	}

	/**
	 * @return image represented as a {@link NiftiImage}
	 */
	@Override
	public BioHipiImage decodeImage(InputStream inputStream, BioHipiImageHeader imageHeader) throws IllegalArgumentException, IOException {
		return new NiftiImage(inputStream, imageHeader);
	}
	
	@Override
	public void encodeImage(BioHipiImage image, OutputStream outputStream) throws IllegalArgumentException, IOException {
		NiftiVolume niiVol = ((NiftiImage) image).getNiftiVolume();
		niiVol.write(outputStream);
	}
}