package org.hipi.image;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.dcm4che3.data.Attributes;
import org.dcm4che3.io.DicomInputStream;
import org.dcm4che3.io.DicomOutputStream;
import org.dcm4che3.io.DicomInputStream.IncludeBulkData;
import org.dcm4che3.util.SafeClose;

public class DicomImage extends HipiImage {

	Attributes fmi;
	Attributes dataset;
	DicomInputStream dis;

	public DicomImage(){
		super();
	}

	public DicomImage(InputStream ip, HipiImageHeader header) throws IOException {
		this.header = header;
		
		setDicomValues(ip);
		
//		dis = new DicomInputStream(ip);
//
//		try {
//			dis.setIncludeBulkData(IncludeBulkData.URI);
//			fmi = dis.readFileMetaInformation();
//			dataset = dis.readDataset(-1, -1);
//		} finally {
//			dis.close();
//		}
	}
	
	public HipiImageType getType() { return HipiImageType.DICOM; }

	public Attributes 		getFmi() 	 		  { return fmi; }
	public Attributes 		getDataset() 		  { return dataset; }
	public DicomInputStream getDicomInputStream() { return dis; }

	@Override
	public void write(DataOutput out) throws IOException {
		header.write(out);
		
		DicomOutputStream dos = null;
		try {
			dos = new DicomOutputStream((OutputStream) out, dis.getTransferSyntax());
			dos.writeDataset(fmi, dataset);
		} finally {
			SafeClose.close(dos);
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		header = new HipiImageHeader(in);

		setDicomValues((InputStream) in);
		
//		dis = new DicomInputStream((InputStream) in);
//		try {
//			dis.setIncludeBulkData(IncludeBulkData.URI);
//			fmi = dis.readFileMetaInformation();
//			dataset = dis.readDataset(-1, -1);
//		} finally {
//			dis.close();
//		}
	}
	
	public void setDicomValues(InputStream ip) throws IOException {
		dis = new DicomInputStream(ip);

		try {
			dis.setIncludeBulkData(IncludeBulkData.URI);
			fmi = dis.readFileMetaInformation();
			dataset = dis.readDataset(-1, -1);
		} finally {
			dis.close();
		}
	}

	@Override
	public String hex() { return null; }
}
