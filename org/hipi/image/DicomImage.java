package org.hipi.image;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.dcm4che3.data.Attributes;
import org.dcm4che3.data.Tag;
import org.dcm4che3.data.UID;
import org.dcm4che3.io.DicomInputStream;
import org.dcm4che3.io.DicomOutputStream;
import org.dcm4che3.io.DicomInputStream.IncludeBulkData;
import org.dcm4che3.util.SafeClose;
import org.hipi.util.DcmDump;

import ij.plugin.DICOM;

/**
 * A DICOM image represented as an dataset of Java Objects.
 *
 * The {@link org.hipi.image.io} package provides classes for reading
 * (decoding) and writing (encoding) DicomImage objects in
 * the format DICOM images.
 * 
 */

public class DicomImage extends HipiImage {
	
	/**
	 * DICOM image byte array
	 */
	ByteArrayOutputStream byteArrayOutputStream;

	/**
	 * File Meta Information.
	 */
	Attributes fmi;
	
	/**
	 * Tag Dataset.
	 */
	Attributes dataset;

	/**
	 * DICOM reader for the 3D structure.
	 */
	DICOM dicom;
	
	public DicomImage() {
		super();
	}
	
	public DicomImage(InputStream ip, HipiImageHeader header) throws IOException {
		this.header = header;
		inizialiteByteArrayOutputStream(ip);
		this.dicom = new DICOM(getInputStream());
		this.dicom.run("w");
		setDicomValues(getInputStream());
	}
	
	/**
	 * @return HipiImageType.DICOM
	 * @see HipiImageType
	 */
	public HipiImageType getType() {
		return HipiImageType.DICOM;
	}

	/**
	 * @return {@link DICOM} instance to create a stack of DICOM images.
	 */
	public DICOM getDICOM() {
		return this.dicom;
	}
	
	/**
	 * @return File Meta Information for encode operations. 
	 * @see {@link Attributes} 
	 */
	public Attributes getFileMetaInformation() {
		return fmi;
	}
	
	/**
	 * @return Dataset for encode operations and extracting
	 * values from specific Tag.
	 * @see {@link Attributes} 
	 */
	public Attributes getDataset() {
		return dataset;
	}
	
	/**
	 * Initializes an array of bytes to be used for
	 * the various read operations.
	 * 
	 * @param ip {@link InputStream}
	 * @throws IOException
	 */
	private void inizialiteByteArrayOutputStream(InputStream ip) throws IOException {
		byteArrayOutputStream = new ByteArrayOutputStream();

		byte[] buffer = new byte[1024];
		int len;
		while ((len = ip.read(buffer)) > -1 )
			byteArrayOutputStream.write(buffer, 0, len);
		byteArrayOutputStream.flush();
	}

	/**
	 * @return {@link InputStream} from DICOM image byte array.
	 * @see #inizialiteByteArrayOutputStream(InputStream)
	 */
	public InputStream getInputStream() {
		return new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
	}

	/**
	 * Set the useful values of DICOM image for write operations.
	 * 
	 * @param ip {@link InputStream}
	 * @throws IOException
	 */
	public void setDicomValues(InputStream ip) throws IOException {
		DicomInputStream dis = new DicomInputStream(ip);
		try {
			dis.setIncludeBulkData(IncludeBulkData.URI);
			fmi = dis.readFileMetaInformation();
			dataset = dis.readDataset(-1, -1);
			fmi = dataset.createFileMetaInformation(dis.getTransferSyntax());
		} finally {
			dis.close();
		}
	}
	
	/**
	 * Get the value of the field through a specific tag.
	 * 
	 * @param integer that identifies the specific tag to be returned. It recommends the use of {@link Tag} enumerated (e.g., Tag.PatientName).
	 * 
	 * @return <ul>
	 * 			<li> <code>String</code> if the value of the consideration to the tag is a String <i>(e.g. PatientName, InstitutionName, ...)</i>.
	 *  		<li> <code>Date</code> if the value of the consideration to the tag is a Date <i>(e.g. PatientBirthDate, StudyDate, ...)</i>.
	 *  		<li> <code>Long</code> if the value of the consideration to the tag is a Long Number <i>(e.g. AccessionNumber, ...)</i>.
	 *  		<li> <code>Double</code> if the value of the consideration to the tag is a Double Number <i>(e.g. ImageOrientationPatient, SliceLocation ...)</i>.
	 * 			<li> <code>Integer</code> if the value of the consideration to the tag is a Integer Number <i>(e.g. PatientAge, PixelBandwidth ...)</i>.
	 * 			<li> <code>Object</code> if the value of the consideration to the tag isn't specified in the function.
	 * 		</ul>
	 * 
	 * @see {@link Tag}
	 */
	public Object getFieldValue(int tag) {
		switch (tag) {
		case Tag.ImplementationVersionName: case Tag.SpecificCharacterSet: case Tag.InstitutionName: 
		case Tag.InstitutionAddress: case Tag.StudyDescription: case Tag.SeriesDescription: 
		case Tag.PatientName: case Tag.PatientID: case Tag.PatientSex: case Tag.PatientWeight: case Tag.ScanningSequence:
		case Tag.SequenceName: case Tag.ImagedNucleus: case Tag.SoftwareVersions:
		case Tag.ProtocolName: case Tag.AcquisitionMatrix: case Tag.InPlanePhaseEncodingDirection: case Tag.FlipAngle:
		case Tag.VariableFlipAngleFlag: case Tag.PatientPosition: case Tag.RequestedProcedureDescription:
		case Tag.PerformedProcedureStepID: case Tag.PerformedProcedureStepDescription:
			return getDataset().getString(tag);

		case Tag.AccessionNumber:
			return Long.parseLong(getDataset().getString(tag));

		case Tag.StudyTime: case Tag.SeriesTime: case Tag.AcquisitionTime:
		case Tag.ImagingFrequency: case Tag.MagneticFieldStrength: case Tag.SpacingBetweenSlices:
		case Tag.PercentPhaseFieldOfView: case Tag.TimeOfLastCalibration: case Tag.ImageOrientationPatient:
		case Tag.SliceLocation: case Tag.PixelSpacing: case Tag.PerformedProcedureStepStartTime:
			return Double.parseDouble(getDataset().getString(tag));

		case Tag.StudyDate: case Tag.PatientBirthDate:
		case Tag.DateOfLastCalibration: case Tag.PerformedProcedureStepStartDate:
			return getDataset().getDate(tag);

		case Tag.PatientAge:
			return Integer.parseInt(getDataset().getString(Tag.PatientAge).substring(0, getDataset().getString(Tag.PatientAge).length()-1));

		case Tag.NumberOfAverages: case Tag.EchoNumbers: case Tag.NumberOfPhaseEncodingSteps: case Tag.PixelBandwidth:
		case Tag.DeviceSerialNumber: case Tag.StudyID: case Tag.SeriesNumber: case Tag.AcquisitionNumber: 
		case Tag.InstanceNumber: case Tag.Rows: case Tag.Columns: case Tag.BitsStored: case Tag.HighBit: case Tag.SmallestImagePixelValue:
			return Integer.parseInt(getDataset().getString(tag));

		default:
			return getDataset().getValue(tag);
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {

		header.write(out);

		DicomOutputStream dos = null;
		try {
			dos = new DicomOutputStream((OutputStream) out, UID.ExplicitVRLittleEndian);
			dos.writeDataset(fmi, dataset);
		} finally {
			SafeClose.close(dos);
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		header = new HipiImageHeader(in);
		
		inizialiteByteArrayOutputStream((InputStream) in);
		this.dicom = new DICOM(getInputStream());
		this.dicom.run("w");
		setDicomValues(getInputStream());
	}

	/**
	 * @return A string representation of the DICOM image as a list of tags.
	 */
	public String toString() {
		
		DcmDump dcmDump = new DcmDump();
		DicomInputStream dis = null;
		try {
			try {
				dis = new DicomInputStream(getInputStream());
				dcmDump.parse(dis);
			} finally {
				dis.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return new String(dcmDump.getStringBuilder());
	}

	@Override
	public String hex() { return null; }
}