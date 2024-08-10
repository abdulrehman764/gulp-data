import os
import jsonlines
import json

# Path to the directory containing JSON files
source_directory = "."
# Path to the directory where merged files will be saved
destination_directory = "merged_files"

# Function to merge JSON objects from files with similar names
def merge_json_files(source_directory, destination_directory):
    # Create the destination directory if it doesn't exist
    if not os.path.exists(destination_directory):
        os.makedirs(destination_directory)
    
    # Dictionary to store JSON objects for each group
    merged_data = {}
    
    # Iterate over files in the source directory
    print("Merging JSON files...")
    for filename in os.listdir(source_directory):
        if filename.endswith(".json") and "_2024" in filename:
            # Extract the prefix before "_2024"
            prefix = filename.split("_2024")[0]
            
            # Read the JSON lines file and extract JSON objects
            with jsonlines.open(os.path.join(source_directory, filename)) as reader:
                for json_obj in reader:
                    merged_data.setdefault(prefix, []).append(json_obj)
    
    # Write merged data to new files in the destination directory
    print("Writing merged data to new files...")
    for prefix, data in merged_data.items():
        with open(os.path.join(destination_directory, f"{prefix}.json"), "w") as outfile:
            # Write JSON objects to the new file separated by new lines
            for json_obj in data:
                json.dump(json_obj, outfile)
                outfile.write("\n")
    
    print("Merging process completed.")

# Call the function to merge JSON files
merge_json_files(source_directory, destination_directory)
