import os
import sys
import logging
import subprocess
import zipfile

# ================== Logging Setup ==================
def setup_logging(log_file):
    logging.basicConfig(
        filename=log_file,
        filemode="a",
        format="%(asctime)s [%(levelname)s] %(message)s",
        level=logging.INFO
    )
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logging.getLogger().addHandler(console)

# ================== Main Extract Function ==================
def main(output_dir):
    dataset = "psparks/instacart-market-basket-analysis"
    raw_dir = os.path.join(output_dir, "raw")
    os.makedirs(raw_dir, exist_ok=True)

    log_file = os.path.join(output_dir, "extract.log")
    setup_logging(log_file)

    logging.info("===== ETL Extract Step Started =====")
    logging.info(f"Dataset: {dataset}")
    logging.info(f"Raw Data Directory: {raw_dir}")

    expected_files = [
        "orders.csv",
        "order_products__prior.csv",
        "order_products__train.csv",
        "products.csv",
        "aisles.csv",
        "departments.csv"
    ]
    missing_files = [f for f in expected_files if not os.path.exists(os.path.join(raw_dir, f))]

    if not missing_files:
        logging.info("All dataset files already present. Skipping download.")
    else:
        logging.info(f"Missing files detected: {missing_files}")
        logging.info("Starting Kaggle dataset download...")

        try:
            subprocess.run(
                ["kaggle", "datasets", "download", "-d", dataset, "-p", raw_dir],
                check=True
            )
            logging.info("Download complete. Extracting zip...")

            # The Kaggle API downloads a zip named after the dataset
            zip_path = os.path.join(raw_dir, dataset.split("/")[-1] + ".zip")
            if not os.path.exists(zip_path):
                # Some Kaggle APIs download with dataset name plus numbers
                zip_files = [f for f in os.listdir(raw_dir) if f.endswith(".zip")]
                if zip_files:
                    zip_path = os.path.join(raw_dir, zip_files[0])
                else:
                    logging.error("No ZIP file found after download.")
                    sys.exit(1)

            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                zip_ref.extractall(raw_dir)
            os.remove(zip_path)
            logging.info("Extraction completed successfully.")

        except Exception as e:
            logging.error(f"Error during download or extraction: {e}")
            sys.exit(1)

    logging.info("===== ETL Extract Step Completed =====")

# ================== Entry Point ==================
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python execute.py <output_directory>")
        sys.exit(1)

    output_directory = sys.argv[1]
    main(output_directory)