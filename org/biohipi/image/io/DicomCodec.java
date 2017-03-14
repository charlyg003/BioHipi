package org.biohipi.image.io;

import org.biohipi.image.BioHipiImageHeader;
import org.biohipi.image.BioHipiImageHeader.BioHipiImageFormat;
import org.biohipi.image.BioHipiImageHeader.BioHipiKeyMetaData;
import org.dcm4che3.data.Attributes;
import org.dcm4che3.data.Tag;
import org.dcm4che3.data.UID;
import org.dcm4che3.io.DicomInputStream;
import org.dcm4che3.io.DicomOutputStream;
import org.dcm4che3.util.SafeClose;
import org.biohipi.image.DicomImage;
import org.biohipi.image.BioHipiImage;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Class for objects that serve as both an {@link ImageDecoder}
 * and {@link ImageEncoder} for the DICOM image storage format.
 */
public class DicomCodec implements ImageDecoder, ImageEncoder {

	private static final DicomCodec staticObject = new DicomCodec();

	public static DicomCodec getInstance() {
		return staticObject;
	}

	public BioHipiImageHeader decodeHeader(InputStream inputStream) throws IOException {

		DicomInputStream dis = new DicomInputStream(inputStream);
		Attributes dataset = null;
		try {
			dataset = dis.readDataset(-1, -1);
		} finally {
			dis.close();
		}

		BioHipiImageHeader header = new BioHipiImageHeader(BioHipiImageFormat.DICOM);
		header.addMetaData(BioHipiKeyMetaData.PATIENT_ID, dataset.getString(Tag.PatientID));
		header.addMetaData(BioHipiKeyMetaData.PATIENT_NAME, dataset.getString(Tag.PatientName));
		header.addMetaData(BioHipiKeyMetaData.ROWS, dataset.getString(Tag.Rows));
		header.addMetaData(BioHipiKeyMetaData.COLUMNS, dataset.getString(Tag.Columns));

		return header;
	}

	/**
	 * @return image represented as a {@link DicomImage}
	 */
	public BioHipiImage decodeHeaderAndImage(InputStream inputStream) throws IOException, IllegalArgumentException {

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		byte[] buffer = new byte[1024];
		int len;
		while ((len = inputStream.read(buffer)) > -1 )
			baos.write(buffer, 0, len);
		baos.flush();

		InputStream ipForHeader = new ByteArrayInputStream(baos.toByteArray());
		InputStream ipForImage  = new ByteArrayInputStream(baos.toByteArray());

		return decodeImage(ipForImage, decodeHeader(ipForHeader));
	}

	/**
	 * @return image represented as a {@link DicomImage}
	 */
	public BioHipiImage decodeImage(InputStream inputStream, BioHipiImageHeader imageHeader) throws IllegalArgumentException, IOException {
		return new DicomImage(inputStream, imageHeader);
	}

	public void encodeImage(BioHipiImage image, OutputStream outputStream) throws IllegalArgumentException, IOException {

		DicomOutputStream dos = null;

		try {
			dos = new DicomOutputStream(outputStream, UID.ExplicitVRLittleEndian);
			dos.writeDataset(((DicomImage)image).getFileMetaInformation(), ((DicomImage)image).getDataset());
		} finally {
			SafeClose.close(dos);
		}
	}
}