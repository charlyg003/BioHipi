package org.biohipi.image;

import java.io.ByteArrayInputStream;
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
import org.biohipi.examples.Voxel3dDicom;
import org.biohipi.util.ByteUtils;
import org.biohipi.util.DcmDump;

import ij.ImageStack;
import ij.plugin.DICOM;

/**
 * A DICOM image represented as an dataset of Java Objects. A DicomImage extends the
 * abstract base class {@link BioHipiImage} and consists of a {@link BioHipiImageHeader}.
 *<br>
 * The {@link org.biohipi.image.io} package provides classes for reading
 * (decoding) and writing (encoding) DicomImage objects in
 * the format DICOM images.
 */
public class DicomImage extends BioHipiImage {

	/**
	 * DICOM image bytes from {@link ByteUtils#inputStreamToByteArray(InputStream)}
	 */
	byte[] imageBytes;

	/**
	 * File Meta Information
	 */
	Attributes fmi;

	/**
	 * Tag Dataset
	 */
	Attributes dataset;

	/**
	 * DICOM reader for the 3D structure
	 */
	DICOM dicom;

	/**
	 * Default constructor.
	 * 
	 *  @see BioHipiImage#BioHipiImage()
	 */
	public DicomImage() {
		super();
	}

	/**
	 * Creates a new DicomImage.
	 * 
	 * @param ip input stream containing serialized image data
	 * @param header with metadata information
	 */
	public DicomImage(InputStream ip, BioHipiImageHeader header) throws IOException {
		this.header = header;
		this.imageBytes = ByteUtils.inputStreamToByteArray(ip);
		setDicomValues(new ByteArrayInputStream(imageBytes));
	}

	/**
	 * Get image type identifier.
	 * 
	 * @return HipiImageType.DICOM
	 * @see BioHipiImageType
	 */
	public BioHipiImageType getType() {
		return BioHipiImageType.DICOM;
	}

	/**
	 * 
	 * Get DICOM Reader for DICOM Image 3D
	 * 
	 * @return {@link DICOM} instance to create a stack of DICOM images.
	 * 
	 * @see DICOM
	 * @see ImageStack
	 * @see Voxel3dDicom
	 */
	public DICOM getDICOM() {
		if (this.dicom == null) {
			this.dicom = new DICOM(getInputStream());
			dicom.run("read");
		}
		return dicom;
	}

	/**
	 * Get File Meta Information for encode operations.
	 * 
	 * @return File Meta Information for encode operations. 
	 * @see {@link Attributes} 
	 */
	public Attributes getFileMetaInformation() {
		return fmi;
	}

	/**
	 * Get Dataset for encode operations and extracting
	 * values from specific Tag.
	 * 
	 * @return Dataset for encode operations and extracting
	 * values from specific Tag.
	 * @see {@link Attributes} 
	 */
	public Attributes getDataset() {
		return dataset;
	}

	/**
	 * Get {@link InputStream} from {@link #imageBytes}.
	 * 
	 * @return {@link InputStream} from {@link #imageBytes}.
	 */
	public InputStream getInputStream() {
		return new ByteArrayInputStream(this.imageBytes);
	}

	/**
	 * Set the useful values of DICOM image for write operations.
	 * 
	 * @param ip input stream containing serialized image data
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
	 * <li> <code>String</code> if the value of the consideration to the tag is a String <i>(e.g. PatientName, InstitutionName, ...)</i>.
	 * <li> <code>Date</code> if the value of the consideration to the tag is a Date <i>(e.g. PatientBirthDate, StudyDate, ...)</i>.
	 * <li> <code>Long</code> if the value of the consideration to the tag is a Long Number <i>(e.g. AccessionNumber, ...)</i>.
	 * <li> <code>Double</code> if the value of the consideration to the tag is a Double Number <i>(e.g. ImageOrientationPatient, SliceLocation ...)</i>.
	 * <li> <code>Integer</code> if the value of the consideration to the tag is a Integer Number <i>(e.g. PatientAge, PixelBandwidth ...)</i>.
	 * <li> <code>Object</code> if the value of the consideration to the tag isn't specified in the function.
	 * </ul>
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

	/*
	 * Serialize the fields of this DicomImage Object to out.
	 * 
	 * @param out DataOuput to serialize this object into.
	 * @see org.apache.hadoop.io.Writable#write
	 */
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

	/**
	 * Deserialize the fields of this DicomImage Object from in.
	 * 
	 * @param in DataInput to deseriablize this object from.
	 * @see org.apache.hadoop.io.Writable#readFields
	 */
	public void readFields(DataInput in) throws IOException {

		header = new BioHipiImageHeader(in);

		this.imageBytes = ByteUtils.inputStreamToByteArray((InputStream) in);
		setDicomValues(getInputStream());
	}

	/**
	 * Produce readable string representation of DICOM Image
	 * 
	 * @return A string representation of the DICOM image as a list of tags.
	 */
	public String toString() {

		DcmDump dcmDump = new DcmDump();
		DicomInputStream dis = null;
		try {
			try {
				dis = new DicomInputStream(new ByteArrayInputStream(imageBytes));
				dcmDump.parse(dis);
			} finally {
				dis.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return new String(dcmDump.getStringBuilder());
	}

}