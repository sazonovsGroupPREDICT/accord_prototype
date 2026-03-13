from pathlib import Path
import subprocess

import dxpy

QUERY_NAME = 'people with rheumatoid arthritis diagnosed before age 50 who later required joint replacement and had CRP > 50 at recruitment'
FIELD_IDS = [34, 53, 20002, 20008, 20009, 21003, 21022, 30710, 30711, 30712, 30713, 30714, 30715, 30716, 41200, 41210, 41270, 41271, 41272, 41280, 41281, 41282, 131848, 131850]
DX_FILE_ID = None
DX_PROJECT_PATH = None  # e.g. "project-xxxx:/Exports/people_with_rheumatoid_arthritis_diagnosed_before_age_50_who_later_requi.tsv.gz"
LOCAL_EXPORT = Path("data/people_with_rheumatoid_arthritis_diagnosed_before_age_50_who_later_requi.tsv.gz")
EXPECTED_DELIMITER = "\t"

# Suggested diagnosis fields: [20002, 41270, 41271]
# Suggested diagnosis anchors: [20002, 41270, 41271, 41280, 41281, 131848, 131850]
# Suggested diagnosis date fields: [41280, 41281, 131848, 131850]
# Suggested diagnosis year fields: [20008]
# Suggested disease-specific diagnosis dates: [131848, 131850]
# Suggested birth year fields: [34]
# Suggested recruitment date fields: [53]
# Suggested recruitment age fields: [21003, 21022]
# Suggested measurement fields: [30710]
# Suggested measurement date fields: [30711]
# Suggested measurement support fields: [30712, 30713, 30714, 30715, 30716]
# Suggested procedure fields: [41200, 41210, 41272]
# Suggested procedure anchors: [41200, 41210, 41272, 41282]
# Suggested record-level access fields: []
# - before age 50
# - Rheumatoid arthritis diagnosis/recorded at or before date of recruitment
# - Crp diagnosis/recorded at or before date of recruitment
# - C-reactive protein > 50 at recruitment

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
