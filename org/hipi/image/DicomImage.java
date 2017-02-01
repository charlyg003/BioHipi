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

import ij.ImageStack;
import ij.plugin.DICOM;

public class DicomImage extends HipiImage {

	DicomInputStream dis;
	Attributes fmi;
	Attributes dataset;
	ByteArrayOutputStream byteArrayOutputStream;

	DICOM dicom;

	public DicomImage(){
		super();
	}

	public DicomImage(InputStream ip, HipiImageHeader header) throws IOException {
		this.header = header;
		
		
		inizialiteByteArrayOutputStream(ip);
		this.dicom = new DICOM(getInputStream());
		this.dicom.run("w");
		setDicomValues(getInputStream());

	}
	
	public DICOM getDICOM() {
		return this.dicom;
	}
	
	public void setDICOMStack(ImageStack dicomStack) {
		this.dicom = new DICOM();
		this.dicom.setStack(dicomStack);
	}
	

	private void inizialiteByteArrayOutputStream(InputStream ip) throws IOException {
		byteArrayOutputStream = new ByteArrayOutputStream();

		byte[] buffer = new byte[1024];
		int len;
		while ((len = ip.read(buffer)) > -1 )
			byteArrayOutputStream.write(buffer, 0, len);
		byteArrayOutputStream.flush();
	}

	public InputStream getInputStream() {
		return new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
	}

	public HipiImageType getType() { return HipiImageType.DICOM; }

	public Attributes		getFileMetaInformation(){ return fmi; }
	public Attributes		getDataset()			{ return dataset; }
	public DicomInputStream getDicomInputStream()	{ return dis; }


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

	public void setDicomValues(InputStream ip) throws IOException {

		dis = new DicomInputStream(ip);

		try {
			dis.setIncludeBulkData(IncludeBulkData.URI);
			fmi = dis.readFileMetaInformation();
			dataset = dis.readDataset(-1, -1);
			fmi = dataset.createFileMetaInformation(dis.getTransferSyntax());
		} finally {
			dis.close();
		}

	}

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


	/**
	 * Get the field value with tag position
	 * 
	 * @param tag position field
	 * @return <code><strong>if</strong> (tag ==  Tag.ImplementationVersionName || Tag.SpecificCharacterSet || Tag.ImageType || Tag.Modality || Tag.Manufacturer
	 * 					|| Tag.InstitutionName || Tag.InstitutionAddress || Tag.StationName || Tag.StudyDescription || Tag.SeriesDescription 
	 * 					|| Tag.ManufacturerModelName || Tag.PatientName || Tag.PatientID || Tag.PatientSex || Tag.PatientWeight 
	 * 					|| Tag.ScanningSequence || Tag.SequenceVariant || Tag.ScanOptions || Tag.MRAcquisitionType || Tag.SequenceName
	 * 					|| Tag.AngioFlag || Tag.ImagedNucleus || Tag.SoftwareVersions || Tag.ProtocolName || Tag.TransmitCoilName
	 * 					|| Tag.AcquisitionMatrix || Tag.InPlanePhaseEncodingDirection || Tag.FlipAngle || Tag.VariableFlipAngleFlag || Tag.PatientPosition
	 * 					|| Tag.PhotometricInterpretation || Tag.RequestedProcedureDescription || Tag.PerformedProcedureStepID || Tag.PerformedProcedureStepDescription)
	 * 			<br><strong>return String</strong><br>
	 *	<br>
	 * 			<strong>else if</strong> (tag ==  Tag.AccessionNumber)
	 * 			<br><strong>return Long</strong><br>
	 *	<br>
	 * 			<strong>else if</strong> (tag ==  Tag.StudyTime|| Tag.SeriesTime || Tag.AcquisitionTime || Tag.ContentTime || Tag.RepetitionTime
	 * 					|| Tag.EchoTime || Tag.InversionTime || Tag.ImagingFrequency || Tag.MagneticFieldStrength || Tag.SpacingBetweenSlices
	 * 					|| Tag.EchoTrainLength || Tag.PercentSampling || Tag.PercentPhaseFieldOfView || Tag.TimeOfLastCalibration || Tag.SAR
	 * 					|| Tag.ImageOrientationPatient || Tag.SliceLocation || Tag.SamplesPerPixel || Tag.PixelSpacing || Tag.PerformedProcedureStepStartTime)
	 * 			<br><strong>return Double</strong><br>
	 *	<br>
	 * 			<strong>else if</strong> (tag == Tag.StudyDate || Tag.PatientBirthDate || Tag.DateOfLastCalibration || Tag.PerformedProcedureStepStartDate)
	 * 			<br><strong>return Date</strong><br>
	 *	<br>
	 * 			<strong>else if</strong> (tag == Tag.PatientAge || Tag.NumberOfAverages || Tag.EchoNumbers || Tag.NumberOfPhaseEncodingSteps
	 * 					|| Tag.PixelBandwidth || Tag.DeviceSerialNumber || Tag.StudyID || Tag.SeriesNumber || Tag.AcquisitionNumber
	 * 					|| Tag.InstanceNumber || Tag.Rows || Tag.Columns || Tag.BitsAllocated || Tag.BitsStored || Tag.HighBit
	 * 					|| Tag.SmallestImagePixelValue || Tag.LargestImagePixelValue)
	 * 			<br><strong>return Integer</strong><br>
	 *	<br>
	 * 			<strong>else</strong>
	 * 			<br><strong>return Object</strong><br></code>
	 */
	public Object getFieldValue(int tag) {
		switch (tag) {
		case Tag.ImplementationVersionName:
		case Tag.SpecificCharacterSet:
		case Tag.ImageType:
		case Tag.Modality:
		case Tag.Manufacturer:
		case Tag.InstitutionName:
		case Tag.InstitutionAddress:
		case Tag.StationName:
		case Tag.StudyDescription:
		case Tag.SeriesDescription:
		case Tag.ManufacturerModelName:
		case Tag.PatientName:
		case Tag.PatientID:
		case Tag.PatientSex:
		case Tag.PatientWeight:
		case Tag.ScanningSequence:
		case Tag.SequenceVariant:
		case Tag.ScanOptions:
		case Tag.MRAcquisitionType:
		case Tag.SequenceName:
		case Tag.AngioFlag:
		case Tag.ImagedNucleus:
		case Tag.SoftwareVersions:
		case Tag.ProtocolName:
		case Tag.TransmitCoilName:
		case Tag.AcquisitionMatrix:
		case Tag.InPlanePhaseEncodingDirection:
		case Tag.FlipAngle:
		case Tag.VariableFlipAngleFlag:
		case Tag.PatientPosition:
		case Tag.PhotometricInterpretation:
		case Tag.RequestedProcedureDescription:
		case Tag.PerformedProcedureStepID:
		case Tag.PerformedProcedureStepDescription:
			return getDataset().getString(tag);

		case Tag.AccessionNumber:
			return Long.parseLong(getDataset().getString(tag));

		case Tag.StudyTime:
		case Tag.SeriesTime:
		case Tag.AcquisitionTime:
		case Tag.ContentTime:
		case Tag.RepetitionTime:
		case Tag.EchoTime:
		case Tag.InversionTime:
		case Tag.ImagingFrequency:
		case Tag.MagneticFieldStrength:
		case Tag.SpacingBetweenSlices:
		case Tag.EchoTrainLength:
		case Tag.PercentSampling:
		case Tag.PercentPhaseFieldOfView:
		case Tag.TimeOfLastCalibration:
		case Tag.SAR:
		case Tag.ImageOrientationPatient:
		case Tag.SliceLocation:
		case Tag.SamplesPerPixel:
		case Tag.PixelSpacing:
		case Tag.PerformedProcedureStepStartTime:
			return Double.parseDouble(getDataset().getString(tag));

		case Tag.StudyDate:
		case Tag.PatientBirthDate:
		case Tag.DateOfLastCalibration:
		case Tag.PerformedProcedureStepStartDate:
			return getDataset().getDate(tag);

		case Tag.PatientAge:
			return Integer.parseInt(getDataset().getString(Tag.PatientAge).substring(0, getDataset().getString(Tag.PatientAge).length()-1));

		case Tag.NumberOfAverages:
		case Tag.EchoNumbers:
		case Tag.NumberOfPhaseEncodingSteps:
		case Tag.PixelBandwidth:
		case Tag.DeviceSerialNumber:
		case Tag.StudyID:
		case Tag.SeriesNumber:
		case Tag.AcquisitionNumber:
		case Tag.InstanceNumber:
		case Tag.Rows:
		case Tag.Columns:
		case Tag.BitsAllocated:
		case Tag.BitsStored:
		case Tag.HighBit:
		case Tag.SmallestImagePixelValue:
		case Tag.LargestImagePixelValue:
			return Integer.parseInt(getDataset().getString(tag));

		default:
			return getDataset().getValue(tag);
		}
	}

	@Override
	public String hex() { return null; }
}