package org.hipi.image;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.hipi.image.HipiImage.HipiImageType;
import org.hipi.image.HipiImageHeader.HipiKeyImageInfo;
import org.hipi.util.niftijio.NiftiVolume;

/**
 * A NIfTI image represented as an 4-dimensional array of Java double numbers.
 *
 * The {@link org.hipi.image.io} package provides classes for reading
 * (decoding) and writing (encoding) NiftiImage objects in
 * the format NIfTI images.
 * 
 */

public class NiftiImage extends HipiImage {

	/**
	 * NIfTI reader/writer
	 */
	private NiftiVolume niiVol;

	public NiftiImage() {
		super();
	}
	
	public NiftiImage(NiftiVolume niiVol, HipiImageHeader imageHeader) {
		this.niiVol = niiVol;
		this.header = imageHeader;
	}

	public NiftiImage(InputStream ip, HipiImageHeader imageHeader) throws FileNotFoundException, IOException {
		this.header = imageHeader;
		niiVol = NiftiVolume.read(ip);
	}

	/**
	 * @return HipiImageType.NIFTI
	 * @see HipiImageType
	 */
	public HipiImageType getType() {
		return HipiImageType.NIFTI;
	}

	/**
	 * @return {@link NiftiVolume} instance to perform read/write operations.
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
		return (Integer)header.getImageInfo(HipiKeyImageInfo.X_LENGTH);
	}
	
	/**
	 * Get y-axis length of image.
	 *
	 * @return y-axis length of image
	 */
	public int getYLength() {
		return (Integer)header.getImageInfo(HipiKeyImageInfo.Y_LENGTH);
	}
	
	/**
	 * Get z-axis length of image.
	 *
	 * @return z-axis length of image
	 */
	public int getZLength() {
		return (Integer)header.getImageInfo(HipiKeyImageInfo.Z_LENGTH);
	}
	
	/**
	 * Get t-axis length of image.
	 *
	 * @return t-axis length of image
	 */
	public int getTLength() {
		return (Integer)header.getImageInfo(HipiKeyImageInfo.T_LENGTH) == 0 ? 1 : (Integer)header.getImageInfo(HipiKeyImageInfo.T_LENGTH);
	}

	/**
	 * Extraction a part of voxels from side to side.
	 * 
	 * @param fromX 
	 * @param fromY
	 * @param fromZ
	 * @param fromT
	 * @param toX
	 * @param toY
	 * @param toZ
	 * @param toT
	 * @return {@link NiftiVolume} instance with a part of voxels of this Nifti Image.
	 */
	public NiftiVolume extractAPart(int fromX, int fromY, int fromZ, int fromT, int toX, int toY, int toZ, int toT) {
		int dimX = toX - fromX == 0 ? 1 : toX - fromX;
		int dimY = toY - fromY == 0 ? 1 : toY - fromY;
		int dimZ = toZ - fromZ == 0 ? 1 : toZ - fromZ;
		int dimT = toT - fromT == 0 ? 1 : toT - fromT;

		if (dimX < 0 || dimY < 0 || dimZ < 0 || dimT < 0)
			throw new IllegalArgumentException(String.format("Invalid space from (%d, %d, %d, %d) to (%d, %d, %d, %d)", 
					fromX, fromY, fromZ, fromT, toX, toY, toZ, toT));

		double[][][][] data = new double[dimX][dimY][dimZ][dimT];

		for (int d = 0, t = fromT; d < dimT; d++, t++)
			for (int k = 0, z = fromZ; k < dimZ; k++, z++)
				for (int j = 0, y = fromY; j < dimY; j++, y++)
					for (int i = 0, x = fromX; i < dimX; i++, x++)
						data [i][j][k][d] = this.niiVol.data[x][y][z][t];

		NiftiVolume out = new NiftiVolume(data);

		return out;
	}
	
	@Override
	public void write(DataOutput output) throws IOException {
		header.write(output);
		niiVol.write((OutputStream)output);
	}

	@Override
	public void readFields(DataInput input) throws IOException {
		header = new HipiImageHeader(input);
		this.niiVol = NiftiVolume.read((InputStream)input);
	}
	
	/**
	 * @return A string representation of the NIfTI image as a list of voxels.
	 */
	public String toString() { 

		StringBuilder stringBuilder = new StringBuilder("NiftiVolume data: \n");

		for (int t = 0; t < getTLength(); t++)
			for (int z = 0; z < getZLength(); z++)
				for (int y = 0; y < getYLength(); y++)
					for (int x = 0; x < getXLength(); x++)
						stringBuilder.append(String.format("\t\t\t\tdata[%d][%d][%d][%d] :\t %f\n", x, y, z, t, niiVol.data[x][y][z][t]) );

		return new String(stringBuilder);
	}

	@Override
	public String hex() { return null; }
}
