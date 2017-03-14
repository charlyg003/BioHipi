package org.biohipi.image;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.biohipi.image.BioHipiImage.BioHipiImageType;
import org.biohipi.image.BioHipiImageHeader.BioHipiKeyMetaData;
import org.biohipi.util.niftijio.NiftiVolume;

/**
 * A NIfTI image represented as an 4-dimensional array of Java doubles. A NiftiImage extends the
 * abstract base class {@link BioHipiImage} and consists of a {@link BioHipiImageHeader}.
 *<br>
 *
 * The {@link org.biohipi.image.io} package provides classes for reading
 * (decoding) and writing (encoding) NiftiImage objects in
 * the format NIfTI images.
 * 
 */

public class NiftiImage extends BioHipiImage {

	/**
	 * NIfTI reader/writer
	 */
	private NiftiVolume niiVol;

	/**
	 * Default constructor.
	 * 
	 *  @see BioHipiImage#BioHipiImage()
	 */
	public NiftiImage() {
		super();
	}

	/**
	 * Creates a new NiftiImage.
	 * 
	 * @param ip input stream containing serialized image data
	 * @param header with meta data information
	 */
	public NiftiImage(InputStream ip, BioHipiImageHeader imageHeader) throws FileNotFoundException, IOException {
		this.header = imageHeader;
		niiVol = NiftiVolume.read(ip);
	}
	
	/**
	 * Creates a new NiftiImage with the data already set.
	 * 
	 * @param niiVol instance of an image already read
	 * @param header with meta data information
	 */
	public NiftiImage(NiftiVolume niiVol, BioHipiImageHeader imageHeader) {
		this.niiVol = niiVol;
		this.header = imageHeader;
	}

	/**
	 * Get image type identifier.
	 * 
	 * @return HipiImageType.NIFTI
	 * @see BioHipiImageType
	 */
	public BioHipiImageType getType() {
		return BioHipiImageType.NIFTI;
	}

	/**
	 * Get {@link NiftiVolume} to perform read/write operations.
	 * 
	 * @return {@link NiftiVolume}.
	 */
	public NiftiVolume getNiftiVolume() {
		return niiVol;
	}

	/**
	 * Get x-axis length of image.
	 *
	 * @return x-axis length of image
	 */
	public int getXLength() {
		return Integer.parseInt(header.getMetaData(BioHipiKeyMetaData.X_LENGTH));
	}

	/**
	 * Get y-axis length of image.
	 *
	 * @return y-axis length of image
	 */
	public int getYLength() {
		return Integer.parseInt(header.getMetaData(BioHipiKeyMetaData.Y_LENGTH));
	}

	/**
	 * Get z-axis length of image.
	 *
	 * @return z-axis length of image
	 */
	public int getZLength() {
		return Integer.parseInt(header.getMetaData(BioHipiKeyMetaData.Z_LENGTH));
	}

	/**
	 * Get t-axis length of image.
	 *
	 * @return t-axis length of image
	 */
	public int getTLength() {
		return Integer.parseInt(header.getMetaData(BioHipiKeyMetaData.T_LENGTH)) == 0 ? 1 : Integer.parseInt(header.getMetaData(BioHipiKeyMetaData.T_LENGTH));
	}

	/**
	 * Cuts a region of the NIfTI image.
	 * 
	 * @param fromX coordinate value x start cutting
	 * @param fromY coordinate value y start cutting
	 * @param fromZ coordinate value z start cutting
	 * @param fromT coordinate value t start cutting
	 * @param xLength length of the x-axis cutting
	 * @param yLength length of the y-axis cutting
	 * @param zLength length of the z-axis cutting
	 * @param tLength length of the t-axis cutting
	 * @return {@link NiftiVolume} with a part of voxels of this NIfTI Image.
	 */
	public NiftiVolume cut(int xStart, int yStart, int zStart, int tStart, int xLength, int yLength, int zLength, int tLength) {

		if (xStart < 0 || yStart < 0 || zStart < 0 || tStart < 0) {
			System.err.println(String.format("Only positive values, entered (%d, %d, %d, %d)", xStart, yStart, zStart, tStart));
			return null;
		}

		if (xStart > this.getXLength() || yStart > this.getYLength() || zStart > this.getZLength() || tStart > this.getTLength()) {
			System.out.println(String.format("Invalid start, max allowed values (%d, %d, %d, %d), values entered (%d, %d, %d, %d)", 
					this.getXLength(), this.getYLength(), this.getZLength(), this.getTLength(), xStart, yStart, zStart, tStart));
			return null;
		}

		int dimX = xStart + xLength;
		int dimY = yStart + yLength;
		int dimZ = zStart + zLength;
		int dimT = tStart + tLength;

		if (dimX > this.getXLength() || dimY > this.getYLength() || dimZ > this.getZLength() || dimT > this.getTLength()) {
			System.err.println(String.format("Invalid space from (%d, %d, %d, %d) to (%d, %d, %d, %d), image length (%d, %d, %d, %d)", 
					xStart, yStart, zStart, tStart, dimX-1, dimY-1, dimZ-1, dimT-1, this.getXLength(), this.getYLength(), this.getZLength(), this.getTLength()));
			return null;
		}

		double[][][][] data = new double[xLength][yLength][zLength][tLength];

		for (int d = 0, t = tStart; d < tLength; d++, t++)
			for (int k = 0, z = zStart; k < zLength; k++, z++)
				for (int j = 0, y = yStart; j < yLength; j++, y++)
					for (int i = 0, x = xStart; i < xLength; i++, x++)
						data [i][j][k][d] = this.niiVol.data[x][y][z][t];

		return new NiftiVolume(data);
	}

	/*
	 * Serialize the fields of this NiftiImage Object to out.
	 * 
	 * @param out DataOuput to serialize this object into.
	 * @see org.apache.hadoop.io.Writable#write
	 */
	public void write(DataOutput output) throws IOException {
		header.write(output);
		niiVol.write((OutputStream)output);
	}

	/**
	 * Deserialize the fields of this NiftiImage Object from in.
	 * 
	 * @param in DataInput to deseriablize this object from. 
	 * @see org.apache.hadoop.io.Writable#readFields
	 */
	public void readFields(DataInput input) throws IOException {
		header = new BioHipiImageHeader(input);
		this.niiVol = NiftiVolume.read((InputStream)input);
	}

	/**
	 * Produce readable string representation of NIfTI Image.
	 * 
	 * @return A string representation of the NIfTI image as a list of voxels
	 */
	public String toString() { 

		StringBuilder stringBuilder = new StringBuilder("NiftiVolume data: \n");

		for (int t = 0; t < getTLength(); t++)
			for (int z = 0; z < getZLength(); z++)
				for (int y = 0; y < getYLength(); y++)
					for (int x = 0; x < getXLength(); x++)
						stringBuilder.append(String.format("\tdata[%d][%d][%d][%d] :\t %f\n", x, y, z, t, niiVol.data[x][y][z][t]) );

		return new String(stringBuilder);
	}

}