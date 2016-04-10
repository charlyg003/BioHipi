package org.myhipi.nifti;

import java.io.ByteArrayInputStream;
import java.io.IOException;

public class MatrixReconstruction {

	@SuppressWarnings("resource")
	public static double[][][] dataReconstruction(Nifti1Dataset nifti) throws IOException{
		short ZZZ;
		int i,j,k;
		
		// for 2D volumes, zdim may be 0
		ZZZ = nifti.ZDIM;
		if (nifti.dim[0] == 2)
			ZZZ = 1;

		short YDIM = nifti.YDIM;
		short XDIM = nifti.XDIM;
		boolean big_endian = nifti.big_endian;
		short datatype = nifti.datatype;
		float scl_slope = nifti.scl_slope;
		float scl_inter = nifti.scl_inter;
		
		// allocate 3D array
		double[][][] data = new double[ZZZ][YDIM][XDIM];

		// read bytes from disk
		
		ByteArrayInputStream bytInpStream = new ByteArrayInputStream(nifti.myData2);

		// read the correct datatypes from the byte array
		// undo signs if necessary, add scaling
		EndianCorrectInputStream ecs = new EndianCorrectInputStream(bytInpStream,big_endian);
		switch (datatype) {

		case Nifti1Dataset.NIFTI_TYPE_INT8:
		case Nifti1Dataset.NIFTI_TYPE_UINT8:
			for (k=0; k<ZZZ; k++)
				for (j=0; j<YDIM; j++)
					for (i=0; i<XDIM; i++) {
						data[k][j][i] = (double) (ecs.readByte());
						if ((datatype == Nifti1Dataset.NIFTI_TYPE_UINT8) && (data[k][j][i] < 0) )
							data[k][j][i] = Math.abs(data[k][j][i]) + (double)(1<<7);
						if (scl_slope != 0)
							data[k][j][i] = data[k][j][i] * scl_slope + scl_inter;
					}
			break;

		case Nifti1Dataset.NIFTI_TYPE_INT16:
		case Nifti1Dataset.NIFTI_TYPE_UINT16:
			for (k=0; k<ZZZ; k++)
				for (j=0; j<YDIM; j++)
					for (i=0; i<XDIM; i++) {
						data[k][j][i] = (double) (ecs.readShortCorrect());
						if ((datatype == Nifti1Dataset.NIFTI_TYPE_UINT16) && (data[k][j][i] < 0))
							data[k][j][i] = Math.abs(data[k][j][i]) + (double)(1<<15);
						if (scl_slope != 0)
							data[k][j][i] = data[k][j][i] * scl_slope + scl_inter;
					}
			break;

		case Nifti1Dataset.NIFTI_TYPE_INT32:
		case Nifti1Dataset.NIFTI_TYPE_UINT32:
			for (k=0; k<ZZZ; k++)
				for (j=0; j<YDIM; j++)
					for (i=0; i<XDIM; i++) {
						data[k][j][i] = (double) (ecs.readIntCorrect());
						if ( (datatype == Nifti1Dataset.NIFTI_TYPE_UINT32) && (data[k][j][i] < 0) )
							data[k][j][i] = Math.abs(data[k][j][i]) + (double)(1<<31);
						if (scl_slope != 0)
							data[k][j][i] = data[k][j][i] * scl_slope + scl_inter;
					}
			break;


		case Nifti1Dataset.NIFTI_TYPE_INT64:
		case Nifti1Dataset.NIFTI_TYPE_UINT64:
			for (k=0; k<ZZZ; k++)
				for (j=0; j<YDIM; j++)
					for (i=0; i<XDIM; i++) {
						data[k][j][i] = (double) (ecs.readLongCorrect());
						if ( (datatype == Nifti1Dataset.NIFTI_TYPE_UINT64) && (data[k][j][i] < 0) )
							data[k][j][i] = Math.abs(data[k][j][i]) + (double)(1<<63);
						if (scl_slope != 0)
							data[k][j][i] = data[k][j][i] * scl_slope + scl_inter;
					}
			break;


		case Nifti1Dataset.NIFTI_TYPE_FLOAT32:
			for (k=0; k<ZZZ; k++)
				for (j=0; j<YDIM; j++)
					for (i=0; i<XDIM; i++) {
						data[k][j][i] = (double) (ecs.readFloatCorrect());
						if (scl_slope != 0)
							data[k][j][i] = data[k][j][i] * scl_slope + scl_inter;
					}
			break;


		case Nifti1Dataset.NIFTI_TYPE_FLOAT64:
			for (k=0; k<ZZZ; k++)
				for (j=0; j<YDIM; j++)
					for (i=0; i<XDIM; i++) {
						data[k][j][i] = (double) (ecs.readDoubleCorrect());
						if (scl_slope != 0)
							data[k][j][i] = data[k][j][i] * scl_slope + scl_inter;
					}
			break;


		case Nifti1Dataset.DT_NONE:
		case Nifti1Dataset.DT_BINARY:
		case Nifti1Dataset.NIFTI_TYPE_COMPLEX64:
		case Nifti1Dataset.NIFTI_TYPE_FLOAT128:
		case Nifti1Dataset.NIFTI_TYPE_RGB24:
		case Nifti1Dataset.NIFTI_TYPE_COMPLEX128:
		case Nifti1Dataset.NIFTI_TYPE_COMPLEX256:
		case Nifti1Dataset.DT_ALL:
		default:
			throw new IOException("Sorry, cannot yet read nifti-1 datatype ");
		}

		ecs.close();
		
		return data;
	}
}
