import os
import pandas as pd
import s3fs
import subprocess

def run_kaggle_etl():
    os.environ['KAGGLE_CONFIG_DIR'] = "/home/ubuntu/.kaggle"

    dataset = "lainguyn123/student-performance-factors"
    data_dir = "/home/ubuntu/airflow/data"
    os.makedirs(data_dir, exist_ok=True)

    try:
        print(f"Dataset URL: https://www.kaggle.com/datasets/{dataset}")
        
       
        subprocess.run(
            ["kaggle", "datasets", "download", "-d", dataset, "-p", data_dir, "--unzip"],
            check=True
        )

        local_file_path = f"{data_dir}/StudentPerformanceFactors.csv"
        s3_file_path = "s3://jahnavi-airflow-bucket/StudentPerformanceFactors.csv"

        df = pd.read_csv(local_file_path)
        df.to_csv(s3_file_path, index=False)
        print(f"Successfully uploaded to {s3_file_path}")

    except FileNotFoundError:
        print("Error: File not found. Ensure the dataset was downloaded.")
    except subprocess.CalledProcessError as e:
        print(f"Error downloading via CLI: {e}")
    except Exception as e:
        print(f"An error occurred during ETL: {e}")

if __name__ == "__main__":
    run_kaggle_etl()
