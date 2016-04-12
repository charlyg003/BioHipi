package org.hipi.image;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import org.hipi.niftijio.NiftiVolume;
import org.myhipi.nifti.Nifti1Dataset;

public class NiftiImage extends HipiImage{
	
	//private Nifti1Dataset nii;
	private NiftiVolume nii;
	
	public NiftiImage(){
		
	}
	
	public NiftiImage(InputStream ip) throws FileNotFoundException, IOException {
//		nii = new Nifti1Dataset();
//		nii.readHeader(ip);
		nii = NiftiVolume.readStream(ip);
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}

	public HipiImageType getType() {
		return HipiImageType.NIFTI;
	}
	
	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String hex() {
		// TODO Auto-generated method stub
		return null;
	}
	
//	public Nifti1Dataset getNifti(){
//		return nii;
//	}
	
	public NiftiVolume getNifti(){
		return nii;
	}
	

}
