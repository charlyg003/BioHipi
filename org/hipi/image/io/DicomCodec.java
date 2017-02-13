package org.hipi.image.io;

import org.hipi.image.HipiImageHeader;
import org.hipi.image.HipiImageHeader.HipiImageFormat;
import org.hipi.image.HipiImageHeader.HipiKeyImageInfo;
import org.dcm4che3.data.Attributes;
import org.dcm4che3.data.Tag;
import org.dcm4che3.data.UID;
import org.dcm4che3.io.DicomInputStream;
import org.dcm4che3.io.DicomOutputStream;
import org.dcm4che3.util.SafeClose;
import org.hipi.image.DicomImage;
import org.hipi.image.HipiImage;
import org.hipi.image.HipiImageFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * Class for objects that serve as both an {@link ImageDecoder}
 * and {@link ImageEncoder} for the DICOM image storage format.
 */
public class DicomCodec implements ImageDecoder, ImageEncoder {

	private static final DicomCodec staticObject = new DicomCodec();

	public static DicomCodec getInstance() {
		return staticObject;
	}
	
	@Override
	public HipiImageHeader decodeHeader(InputStream inputStream) throws IOException {
		return decodeHeader(inputStream, false);
	}

	@Override
	public HipiImageHeader decodeHeader(InputStream inputStream, boolean includeExifData) throws IOException {

		DicomInputStream dis = new DicomInputStream(inputStream);
		Attributes dataset = null;
		try {
			dataset = dis.readDataset(-1, -1);
		} finally {
			dis.close();
		}

		Map<HipiKeyImageInfo, Object> values = new HashMap<HipiKeyImageInfo, Object>();
		values.put(HipiKeyImageInfo.PATIENT_ID, dataset.getString(Tag.PatientID));
		values.put(HipiKeyImageInfo.PATIENT_NAME, dataset.getString(Tag.PatientName));
		values.put(HipiKeyImageInfo.ROWS, Integer.parseInt(dataset.getString(Tag.Rows)));
		values.put(HipiKeyImageInfo.COLUMNS, Integer.parseInt(dataset.getString(Tag.Columns)));
		
		return new HipiImageHeader(HipiImageFormat.DICOM, values,  null , null);
	}

	@Override
	public HipiImage decodeHeaderAndImage(InputStream inputStream, HipiImageFactory imageFactory,
			boolean includeExifData) throws IOException, IllegalArgumentException {

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		byte[] buffer = new byte[1024];
		int len;
		while ((len = inputStream.read(buffer)) > -1 )
			baos.write(buffer, 0, len);
		baos.flush();

		InputStream ipForHeader = new ByteArrayInputStream(baos.toByteArray());
		InputStream ipForImage  = new ByteArrayInputStream(baos.toByteArray());

		HipiImageHeader header = decodeHeader(ipForHeader);
		return decodeImage(ipForImage, header, imageFactory, false);
	}

	/**
	 * @return image represented as a {@link DicomImage}
	 */
	@Override
	public HipiImage decodeImage(InputStream inputStream, HipiImageHeader imageHeader, HipiImageFactory imageFactory,
			boolean includeExifData) throws IllegalArgumentException, IOException {
		return new DicomImage(inputStream, imageHeader);
	}

	@Override
	public void encodeImage(HipiImage image, OutputStream outputStream) throws IllegalArgumentException, IOException {

		DicomOutputStream dos = null;

		try {
			dos = new DicomOutputStream(outputStream, UID.ExplicitVRLittleEndian);
			dos.writeDataset(((DicomImage)image).getFileMetaInformation(), ((DicomImage)image).getDataset());
		} finally {
			SafeClose.close(dos);
		}
	}
}