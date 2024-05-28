import pathlib
import dicom2nifti
import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-i",
        "--input_folder",
        required=True,
        help="input folder where the DICOM images have been uploaded"
    )
    args = parser.parse_args()

    INPUT_FOLDER = pathlib.Path(f"./DICOM/{args.input_folder}")
    INPUT_FOLDER.mkdir(parents=True, exist_ok=True)
    OUTPUT_FOLDER = pathlib.Path("./NIFTI")
    OUTPUT_FOLDER.mkdir(parents=True, exist_ok=True)
    dicom2nifti.dicom_series_to_nifti(
        original_dicom_directory=INPUT_FOLDER,
        output_file=OUTPUT_FOLDER / f"{args.input_folder}.nii"
    )
    print(f"DICOM files: {INPUT_FOLDER}")
    print(f"NIFTI file: {OUTPUT_FOLDER}/{args.input_folder}.nii")
    print("Success")
