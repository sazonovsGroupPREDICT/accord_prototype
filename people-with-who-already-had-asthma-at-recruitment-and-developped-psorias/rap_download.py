from pathlib import Path
import subprocess

import dxpy

QUERY_NAME = 'people with who already had asthma at recruitment and developped psoriasis after'
FIELD_IDS = [34, 53, 3786, 20002, 21003, 21022, 22127, 22147, 41270, 41271, 42014, 131494, 131742]
DX_FILE_ID = None
DX_PROJECT_PATH = None  # e.g. "project-xxxx:/Exports/people_with_who_already_had_asthma_at_recruitment_and_developped_psorias.tsv.gz"
LOCAL_EXPORT = Path("data/people_with_who_already_had_asthma_at_recruitment_and_developped_psorias.tsv.gz")
EXPECTED_DELIMITER = "\t"

# Suggested diagnosis fields: [20002, 41270, 41271]
# Suggested diagnosis anchors: [3786, 20002, 22127, 22147, 41270, 41271, 42014, 131494, 131742]
# Suggested diagnosis date fields: [42014, 131494, 131742]
# Suggested diagnosis year fields: []
# Suggested disease-specific diagnosis dates: [42014, 131494, 131742]
# Suggested birth year fields: [34]
# Suggested recruitment date fields: [53]
# Suggested recruitment age fields: [21003, 21022]
# Suggested measurement fields: []
# Suggested measurement date fields: []
# Suggested measurement support fields: []
# Suggested procedure fields: []
# Suggested procedure anchors: []
# Suggested record-level access fields: []
# - Asthma diagnosis/recorded at or before date of recruitment
# - Psoriasis diagnosis/recorded after recruitment date

def download_export(dx_file_id=DX_FILE_ID, project_path=DX_PROJECT_PATH, local_path=LOCAL_EXPORT):
    local_path = Path(local_path)
    local_path.parent.mkdir(parents=True, exist_ok=True)

    if dx_file_id:
        dxpy.download_dxfile(dxid=dx_file_id, filename=str(local_path))
    elif project_path:
        subprocess.run(["dx", "download", project_path, "-o", str(local_path)], check=True)
    else:
        raise ValueError("Set DX_FILE_ID or DX_PROJECT_PATH to the exported RAP phenotype file.")

    return local_path


if __name__ == "__main__":
    downloaded = download_export()
    print("Downloaded:", downloaded)
    print("Planned field IDs:", FIELD_IDS)
