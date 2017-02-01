package org.hipi.image;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.hipi.util.niftijio.NiftiVolume;

public class NiftiImage extends HipiImage {

	private NiftiVolume niiVol;

	public NiftiImage(){
		super();
	}

	public NiftiImage(NiftiVolume niiVol, HipiImageHeader imageHeader) {
		this.niiVol = niiVol;
		this.header = imageHeader;
	}

	public NiftiImage(InputStream ip, HipiImageHeader imageHeader) throws FileNotFoundException, IOException {
		this.header = imageHeader;
		niiVol = NiftiVolume.readStream(ip);
	}

	public HipiImageType getType() { return HipiImageType.NIFTI; }

	public NiftiVolume getNiftiVolume() { return niiVol; }

	public int getDimX() { return (Integer)header.getValue(HipiImageHeader.NIFTI_INDEX_DIM_X); }
	public int getDimY() { return (Integer)header.getValue(HipiImageHeader.NIFTI_INDEX_DIM_Y); }
	public int getDimZ() { return (Integer)header.getValue(HipiImageHeader.NIFTI_INDEX_DIM_Z); }
	public int getDimT() { return (Integer)header.getValue(HipiImageHeader.NIFTI_INDEX_DIM_T) == 0 ? 1 : (Integer)header.getValue(HipiImageHeader.NIFTI_INDEX_DIM_T); }


	public static NiftiVolume extractAPart(NiftiVolume niiVol, int fromX, int fromY, int fromZ, int fromT, int toX, int toY, int toZ, int toT) {
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
						data [i][j][k][d] = niiVol.data[x][y][z][t];

		NiftiVolume out = new NiftiVolume(data);

		return out;
	}

	public String toString() { 

		StringBuilder stringBuilder = new StringBuilder("NiftiVolume data: \n");

		for (int t = 0; t < getDimT(); t++)
			for (int z = 0; z < getDimZ(); z++)
				for (int y = 0; y < getDimY(); y++)
					for (int x = 0; x < getDimX(); x++)
						stringBuilder.append(String.format("\t\t\t\tdata[%d][%d][%d][%d] :\t %f\n", x, y, z, t, niiVol.data[x][y][z][t]) );

		return new String(stringBuilder);
	}


	@Override
	public void write(DataOutput output) throws IOException {
		header.write(output);
		niiVol.write((OutputStream)output);
	}

	@Override
	public void readFields(DataInput input) throws IOException {
		header = new HipiImageHeader(input);
		this.niiVol = NiftiVolume.readStream((InputStream)input);
	}

	@Override
	public String hex() { return null; }
}
