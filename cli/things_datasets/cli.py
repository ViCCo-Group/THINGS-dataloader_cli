import csv
import zipfile
import shutil
import os
import requests
from pathlib import Path
import argparse
import subprocess

def load_datasets():
    datasets = {}
    with open('static/datasets.csv', mode='r') as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            name = row['name']
            sub_dataset_name = row['sub-dataset name']
            description = row['description']
            files = row['files'].split('; ')
            download_url = row['download_url']
            size = row['size']
            include_files = row['include_files'].split('; ')
            code = row.get('code', '')  # Fetch the code, if present

            if name not in datasets:
                datasets[name] = []

            datasets[name].append({
                'sub_dataset_name': sub_dataset_name,
                'description': description,
                'files': files,
                'download_url': download_url,
                'size': size,
                'folder_name': f"{name}_{sub_dataset_name.replace(' ', '_')}",
                'include_files': include_files,
                'code': code
            })
    return datasets

def load_descriptions():
    descriptions = {}
    with open('static/dataset_descriptions.csv', mode='r') as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            name = row['name']
            name_description = row['name_description']
            descriptions[name] = name_description
    return descriptions

def extract_and_rename_zip(zip_path, extract_to, new_folder_name):
    try:
        temp_extract_to = os.path.join(extract_to, 'temp_extract')
        os.makedirs(temp_extract_to, exist_ok=True)

        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(temp_extract_to)

        top_level_dir = os.listdir(temp_extract_to)[0]
        original_top_level_dir = os.path.join(temp_extract_to, top_level_dir)
        new_path = os.path.join(extract_to, new_folder_name)
        
        shutil.move(original_top_level_dir, new_path)
        shutil.rmtree(temp_extract_to)

    except zipfile.BadZipFile as e:
        print(f"Error extracting {zip_path}: {e}")
    except FileNotFoundError as e:
        print(f"File not found: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")

def zip_all_folders(source_dir, output_zip):
    try:
        if not os.path.exists(source_dir) or not os.listdir(source_dir):
            raise Exception("Source directory does not exist or is empty.")

        print(f"Zipping contents of {source_dir} into {output_zip}")  # Debug print

        with zipfile.ZipFile(output_zip, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for foldername, subfolders, filenames in os.walk(source_dir):
                print(f"Processing folder: {foldername}")  # Debug: Show folder being processed
                for filename in filenames:
                    file_path = os.path.join(foldername, filename)
                    arcname = os.path.relpath(file_path, source_dir)
                    zipf.write(file_path, arcname)
                    print(f"Added file: {file_path}")  # Debug: Show each file added

        print(f"Successfully created {output_zip}. Size: {os.path.getsize(output_zip)} bytes")

    except Exception as e:
        print(f"Error creating zip file: {e}")
        raise

def download_file(url, output_path):
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        with open(output_path, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
    except requests.RequestException as e:
        print(f"Error downloading file from {url}: {e}")
        raise

def get_filename_from_response(response):
    content_disposition = response.headers.get('Content-Disposition')
    if content_disposition:
        parts = content_disposition.split(';')
        for part in parts:
            if 'filename=' in part:
                filename = part.split('=')[-1].strip('"')
                return filename
    return None

def download_dataset_openneuro(dataset_id, include_files, download_path):
    try:
        os.makedirs(download_path, exist_ok=True)
        command = ['openneuro-py', 'download', f'--dataset={dataset_id}', f'--target-dir={download_path}']
        if include_files:
            for include_file in include_files:
                command.append(f'--include={include_file}')
        
        print(f"Running command: {' '.join(command)}")  # Debug: Print the command
        result = subprocess.run(command, capture_output=True, text=True)
        
        # Check if the download command was successful
        if result.returncode != 0:
            print(f"Error downloading dataset {dataset_id}: {result.stderr}")
            raise Exception(f"Download failed: {result.stderr}")

        # Check if the directory is empty
        if not os.listdir(download_path):
            raise Exception("Download directory is empty. No files were downloaded.")
        
        print(f"Successfully downloaded dataset {dataset_id} to {download_path}")
    
    except Exception as e:
        print(f"Error during download: {e}")
        raise

def create_readme(selected_urls, datasets, descriptions, readme_path):
    with open(readme_path, 'w') as f:
        for url in selected_urls:
            dataset_info = next(
                (item for items in datasets.values() for item in items if item['download_url'] == url),
                None
            )
            if dataset_info:
                name = next((k for k, v in datasets.items() if dataset_info in v), None)
                if name:
                    f.write(f"Dataset: {name}\n")
                    f.write(f"Sub-Dataset: {dataset_info['sub_dataset_name']}\n")
                    f.write(f"Description: {dataset_info['description']}\n")
                    f.write(f"Size: {dataset_info['size']}\n")
                    f.write(f"Download URL: {url}\n")
                    f.write(f"Files: {', '.join(dataset_info['files'])}\n")
                    f.write(f"Code: {dataset_info['code']}\n\n")

def zip_all_folders(source_dir, output_zip):
    with zipfile.ZipFile(output_zip, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for foldername, subfolders, filenames in os.walk(source_dir):
            for filename in filenames:
                file_path = os.path.join(foldername, filename)
                arcname = os.path.relpath(file_path, source_dir)
                zipf.write(file_path, arcname)
    print(f"Successfully created {output_zip}.")

def main():
    parser = argparse.ArgumentParser(description='Download and package THINGS datasets.')
    parser.add_argument('output_dir', type=str, help='Directory to store the final zip folder and temporary files.')
    args = parser.parse_args()

    output_dir = args.output_dir
    datasets = load_datasets()
    descriptions = load_descriptions()

    print("Available Datasets:\n")
    for idx, (name, info) in enumerate(datasets.items(), start=1):
        print(f"{idx}. {name}")
        print(f"   Description: {descriptions.get(name, 'No description')}")
        for sub_idx, sub in enumerate(info, start=1):
            print(f"      {idx}.{sub_idx}. {sub['sub_dataset_name']}")
            print(f"         Size: {sub['size']}")
            print(f"         Description: {sub['description']}")
    print()

    selection = input("Enter the numbers of the datasets you want to download (e.g., 1.1, 1.2): ").split(',')

    selected_urls = []
    folder_names = []
    for sel in selection:
        try:
            main_idx, sub_idx = map(int, sel.split('.'))
            main_key = list(datasets.keys())[main_idx - 1]
            sub_dataset = datasets[main_key][sub_idx - 1]
            selected_urls.append(sub_dataset['download_url'])
            folder_name = sub_dataset['folder_name']
            folder_names.append(folder_name)
        except (ValueError, IndexError):
            print(f"Invalid selection: {sel}. Skipping.")

    if not selected_urls:
        print("No valid datasets selected. Exiting.")
        return

    download_dir = os.path.join(output_dir, 'downloads')
    extracted_dir = os.path.join(output_dir, 'extracted')
    os.makedirs(download_dir, exist_ok=True)
    os.makedirs(extracted_dir, exist_ok=True)

    extracted_folders = []

    for url, folder_name in zip(selected_urls, folder_names):
        files = next(
            (item['files'] for items in datasets.values() for item in items if item['download_url'] == url),
            None
        )
        include_files = next(
            (item.get('include_files') for items in datasets.values() for item in items if item['download_url'] == url),
            None
        )

        if 'figshare' in url:
            zip_path = os.path.join(download_dir, folder_name + '.zip')
            print(f"Downloading {zip_path} from {url}...")
            download_file(url, zip_path)
            print(f"Extracting {zip_path}...")
            extract_and_rename_zip(zip_path, extracted_dir, folder_name)
            extracted_folders.append(os.path.join(extracted_dir, folder_name))

        elif 'osf' in url:
            folder_path = os.path.join(extracted_dir, folder_name)
            os.makedirs(folder_path, exist_ok=True)
            response = requests.get(url, stream=True)
            response.raise_for_status()
            filename = get_filename_from_response(response)
            if filename is None:
                filename = url.split('/')[-1]
            file_path = os.path.join(folder_path, filename)
            print(f"Downloading {file_path} from {url}...")
            download_file(url, file_path)
            extracted_folders.append(folder_path)

        elif 'openneuro' in url:
            dataset_id = url.split('/')[-1]
            target_folder = os.path.join(extracted_dir, folder_name)
            print(f"Downloading OpenNeuro dataset {dataset_id} into {target_folder}...")
            try:
                os.makedirs(target_folder, exist_ok=True)
                download_dataset_openneuro(dataset_id, include_files, target_folder)
                extracted_folders.append(target_folder)
            except Exception as e:
                print(f"Error downloading OpenNeuro dataset: {e}")

    readme_path = os.path.join(extracted_dir, 'README.txt')
    create_readme(selected_urls, datasets, descriptions, readme_path)

    main_zip_path = os.path.join(output_dir, 'things-datasets.zip')
    zip_all_folders(extracted_dir, main_zip_path)

    shutil.rmtree(download_dir)
    shutil.rmtree(extracted_dir)

    print(f"All selected datasets are packaged into {main_zip_path}.")

if __name__ == '__main__':
    main()