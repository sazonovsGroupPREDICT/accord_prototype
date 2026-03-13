from pathlib import Path
import re

import pandas as pd

DATA_PATH = Path("data/people_with_rheumatoid_arthritis_diagnosed_before_age_50_who_later_requi.tsv.gz")
DELIMITER = "\t"
ID_COLUMN_CANDIDATES = ["eid", "participant.eid", "f.eid", "p.eid"]

FIELD_IDS = [34, 53, 20002, 20008, 20009, 21003, 21022, 30710, 30711, 30712, 30713, 30714, 30715, 30716, 41200, 41210, 41270, 41271, 41272, 41280, 41281, 41282, 131848, 131850]
DIAGNOSIS_FIELD_IDS = [20002, 41270, 41271]
DIAGNOSIS_ANCHOR_FIELD_IDS = [20002, 41270, 41271, 41280, 41281, 131848, 131850]
DIAGNOSIS_DATE_FIELD_IDS = [41280, 41281, 131848, 131850]
DIAGNOSIS_SPECIFIC_DATE_FIELD_IDS = [131848, 131850]
DIAGNOSIS_YEAR_FIELD_IDS = [20008]
DIAGNOSIS_AGE_FIELD_IDS = [20009]
BIRTH_YEAR_FIELD_IDS = [34]
RECRUITMENT_DATE_FIELD_IDS = [53]
RECRUITMENT_AGE_FIELD_IDS = [21003, 21022]
MEASUREMENT_FIELD_IDS = [30710]
MEASUREMENT_DATE_FIELD_IDS = [30711]
MEASUREMENT_SUPPORT_FIELD_IDS = [30712, 30713, 30714, 30715, 30716]
PROCEDURE_FIELD_IDS = [41200, 41210, 41272]
PROCEDURE_ANCHOR_FIELD_IDS = [41200, 41210, 41272, 41282]
PROCEDURE_DATE_FIELD_IDS = [41282]

DIAGNOSIS_EXACT_CODES = ['1464', '71423', '71424']
DIAGNOSIS_PREFIXES = ['7140', 'M05', 'M06']
PROCEDURE_EXACT_CODES = ['O060', 'O061', 'O062', 'O063', 'O068', 'O069', 'O070', 'O071', 'O072', 'O073', 'O078', 'O079', 'O080', 'O081', 'O082', 'O083', 'O084', 'O088', 'O089', 'O180', 'O181', 'O182', 'O183', 'O184', 'O188', 'O189', 'O210', 'O211', 'O212', 'O213', 'O214', 'O218', 'O219', 'O220', 'O221', 'O222', 'O223', 'O224', 'O228', 'O229', 'O230', 'O231', 'O232', 'O233', 'O234', 'O235', 'O238', 'O239', 'O320', 'O321', 'O322', 'O323', 'O324', 'O325', 'O370', 'O371', 'O372', 'O378', 'O379', 'O380', 'O381', 'O382', 'O388', 'O389', 'O390', 'O391', 'O392', 'O393', 'O398', 'O399', 'O400', 'O401', 'O402', 'O403', 'O404', 'O408', 'O409', 'V201', 'V202', 'W370', 'W371', 'W372', 'W373', 'W374', 'W378', 'W379', 'W380', 'W381', 'W382', 'W383', 'W384', 'W388', 'W389', 'W390', 'W391', 'W392', 'W393', 'W394', 'W395', 'W396', 'W398', 'W399', 'W400', 'W401', 'W402', 'W403', 'W404', 'W408', 'W409', 'W410', 'W411', 'W412', 'W413', 'W414', 'W418', 'W419', 'W420', 'W421', 'W422', 'W423', 'W424', 'W425', 'W426', 'W428', 'W429', 'W430', 'W431', 'W432', 'W433', 'W434', 'W438', 'W439', 'W440', 'W441', 'W442', 'W443', 'W444', 'W448', 'W449', 'W450', 'W451', 'W452', 'W453', 'W454', 'W455', 'W458', 'W459', 'W591', 'W930', 'W931', 'W932', 'W933', 'W938', 'W939', 'W940', 'W941', 'W942', 'W943', 'W948', 'W949', 'W950', 'W951', 'W952', 'W953', 'W954', 'W958', 'W959', 'W960', 'W961', 'W962', 'W963', 'W964', 'W965', 'W966', 'W968', 'W969', 'W970', 'W971', 'W972', 'W973', 'W974', 'W975', 'W976', 'W978', 'W979', 'W980', 'W981', 'W982', 'W983', 'W984', 'W985', 'W986', 'W987', 'W988', 'W989']
PROCEDURE_PREFIXES = []

AGE_LIMIT = 50
SEQUENCE_REQUIRED = True
MEASUREMENT_CONSTRAINT_LABEL = 'C-reactive protein'
MEASUREMENT_CONSTRAINT_OPERATOR = '>'
MEASUREMENT_CONSTRAINT_VALUE = 50.0
CONDITION_RULES = []


def load_table(path=DATA_PATH):
    if str(path).endswith(".parquet"):
        return pd.read_parquet(path)
    return pd.read_csv(path, sep=DELIMITER, low_memory=False)


def columns_for_field(df, field_id):
    patterns = [
        re.compile(rf"(^|[._]){field_id}([._]|$)", re.I),
        re.compile(rf"(^|[._])f[._]?{field_id}([._]|$)", re.I),
        re.compile(rf"(^|[._])p[._]?{field_id}([._]|$)", re.I),
    ]
    return [column for column in df.columns if any(pattern.search(column) for pattern in patterns)]


def first_existing(columns, candidates):
    for candidate in candidates:
        if candidate in columns:
            return candidate
    return None


def first_numeric(df, field_ids):
    columns = [column for field_id in field_ids for column in columns_for_field(df, field_id)]
    if not columns:
        return pd.Series(pd.NA, index=df.index, dtype="float64"), columns
    values = df[columns].apply(pd.to_numeric, errors="coerce")
    return values.min(axis=1), columns


def first_datetime(df, field_ids):
    columns = [column for field_id in field_ids for column in columns_for_field(df, field_id)]
    if not columns:
        return pd.Series(pd.NaT, index=df.index), columns
    values = df[columns].apply(pd.to_datetime, errors="coerce")
    return values.min(axis=1), columns


def any_matching_code(df, field_ids, exact_codes=(), prefixes=()):
    columns = [column for field_id in field_ids for column in columns_for_field(df, field_id)]
    if not columns:
        return pd.Series(False, index=df.index), columns

    exact = {str(code).upper().strip() for code in exact_codes if str(code).strip()}
    prefix_tuple = tuple(str(prefix).upper().strip() for prefix in prefixes if str(prefix).strip())
    values = df[columns].fillna("").astype(str).apply(lambda col: col.str.upper().str.strip())
    mask = pd.DataFrame(False, index=df.index, columns=columns)

    if exact:
        mask = mask | values.isin(exact)
    if prefix_tuple:
        mask = mask | values.apply(lambda col: col.str.startswith(prefix_tuple))

    return mask.any(axis=1), columns


def any_positive_flag(df, field_ids):
    columns = [column for field_id in field_ids for column in columns_for_field(df, field_id)]
    if not columns:
        return pd.Series(False, index=df.index), columns

    raw = df[columns]
    numeric = raw.apply(pd.to_numeric, errors="coerce").gt(0).fillna(False)
    text = raw.fillna("").astype(str).apply(lambda col: col.str.strip().str.lower())
    text_mask = text.isin({"1", "1.0", "yes", "true", "y", "positive", "present"})
    return (numeric | text_mask).any(axis=1), columns


df = load_table()
id_column = first_existing(df.columns, ID_COLUMN_CANDIDATES)

diagnosis_case, diagnosis_code_columns = any_matching_code(
    df,
    DIAGNOSIS_FIELD_IDS,
    exact_codes=DIAGNOSIS_EXACT_CODES,
    prefixes=DIAGNOSIS_PREFIXES,
)

procedure_case, procedure_code_columns = any_matching_code(
    df,
    PROCEDURE_FIELD_IDS,
    exact_codes=PROCEDURE_EXACT_CODES,
    prefixes=PROCEDURE_PREFIXES,
)

diagnosis_date, diagnosis_date_columns = first_datetime(df, DIAGNOSIS_DATE_FIELD_IDS)
diagnosis_specific_date, diagnosis_specific_date_columns = first_datetime(
    df, DIAGNOSIS_SPECIFIC_DATE_FIELD_IDS
)
diagnosis_year, diagnosis_year_columns = first_numeric(df, DIAGNOSIS_YEAR_FIELD_IDS)
procedure_date, procedure_date_columns = first_datetime(df, PROCEDURE_DATE_FIELD_IDS)
diagnosis_age, diagnosis_age_columns = first_numeric(df, DIAGNOSIS_AGE_FIELD_IDS)
birth_year, birth_year_columns = first_numeric(df, BIRTH_YEAR_FIELD_IDS)
recruitment_date, recruitment_date_columns = first_datetime(df, RECRUITMENT_DATE_FIELD_IDS)
recruitment_age, recruitment_age_columns = first_numeric(df, RECRUITMENT_AGE_FIELD_IDS)
measurement_value, measurement_value_columns = first_numeric(df, MEASUREMENT_FIELD_IDS)
measurement_date, measurement_date_columns = first_datetime(df, MEASUREMENT_DATE_FIELD_IDS)
measurement_support_columns = [
    column
    for field_id in MEASUREMENT_SUPPORT_FIELD_IDS
    for column in columns_for_field(df, field_id)
]

primary_diagnosis_date = diagnosis_specific_date.combine_first(diagnosis_date)
derived_age_from_date = primary_diagnosis_date.dt.year - birth_year
derived_age_from_year = diagnosis_year - birth_year
age_at_diagnosis = diagnosis_age.combine_first(derived_age_from_date).combine_first(derived_age_from_year)

if not diagnosis_case.any() and not (DIAGNOSIS_EXACT_CODES or DIAGNOSIS_PREFIXES):
    diagnosis_case = diagnosis_specific_date.notna()

if AGE_LIMIT is None:
    age_mask = pd.Series(True, index=df.index)
else:
    age_mask = age_at_diagnosis.lt(AGE_LIMIT)

if MEASUREMENT_CONSTRAINT_VALUE is None:
    measurement_mask = pd.Series(True, index=df.index)
else:
    measurement_mask = measurement_value.notna()
    if MEASUREMENT_CONSTRAINT_OPERATOR == ">":
        measurement_mask = measurement_mask & measurement_value.gt(MEASUREMENT_CONSTRAINT_VALUE)
    elif MEASUREMENT_CONSTRAINT_OPERATOR == ">=":
        measurement_mask = measurement_mask & measurement_value.ge(MEASUREMENT_CONSTRAINT_VALUE)
    elif MEASUREMENT_CONSTRAINT_OPERATOR == "<":
        measurement_mask = measurement_mask & measurement_value.lt(MEASUREMENT_CONSTRAINT_VALUE)
    elif MEASUREMENT_CONSTRAINT_OPERATOR == "<=":
        measurement_mask = measurement_mask & measurement_value.le(MEASUREMENT_CONSTRAINT_VALUE)
    else:
        raise ValueError("Unsupported measurement operator: %r" % (MEASUREMENT_CONSTRAINT_OPERATOR,))

if CONDITION_RULES:
    summary_values = {"n_total": len(df)}
    condition_masks = []
    condition_selected_columns = []
    condition_outputs = []

    for index, rule in enumerate(CONDITION_RULES, start=1):
        condition_code_mask, condition_code_columns = any_matching_code(
            df,
            rule.get("diagnosis_field_ids", []),
            exact_codes=rule.get("exact_codes", ()),
            prefixes=rule.get("prefixes", ()),
        )
        condition_anchor_mask, condition_anchor_columns = any_positive_flag(
            df,
            rule.get("anchor_field_ids", []),
        )
        condition_date, condition_date_columns = first_datetime(
            df,
            rule.get("date_field_ids", []),
        )
        condition_age, condition_age_columns = first_numeric(
            df,
            rule.get("age_field_ids", []),
        )

        condition_case = (
            condition_code_mask
            | condition_anchor_mask
            | condition_date.notna()
            | condition_age.notna()
        )

        relation = str(rule.get("relation", "")).strip()
        if relation == "on_or_before_recruitment":
            condition_temporal_mask = (
                (condition_date.notna() & recruitment_date.notna() & condition_date.le(recruitment_date))
                | (condition_age.notna() & recruitment_age.notna() & condition_age.le(recruitment_age))
            )
        elif relation == "after_recruitment":
            condition_temporal_mask = (
                (condition_date.notna() & recruitment_date.notna() & condition_date.gt(recruitment_date))
                | (condition_age.notna() & recruitment_age.notna() & condition_age.gt(recruitment_age))
            )
        else:
            condition_temporal_mask = pd.Series(True, index=df.index)

        if relation in {"on_or_before_recruitment", "after_recruitment"} and not (
            condition_date.notna().any() or condition_age.notna().any()
        ):
            print("Warning: no usable timing fields were configured for", rule.get("label", f"condition_{index}"))

        label_slug = str(rule.get("slug") or f"condition_{index}")
        summary_values[f"n_{label_slug}_case"] = int(condition_case.sum())
        summary_values[f"n_{label_slug}_timeline_filtered"] = int(
            (condition_case & condition_temporal_mask).sum()
        )

        condition_masks.append(condition_case & condition_temporal_mask)
        condition_selected_columns.extend(condition_code_columns)
        condition_selected_columns.extend(condition_anchor_columns)
        condition_selected_columns.extend(condition_date_columns)
        condition_selected_columns.extend(condition_age_columns)
        condition_outputs.append((label_slug, condition_date, condition_age))

    timeline_mask = pd.Series(True, index=df.index)
    for condition_mask in condition_masks:
        timeline_mask = timeline_mask & condition_mask

    cohort_mask = timeline_mask & measurement_mask
    summary_values["n_timeline_filtered"] = int(timeline_mask.sum())
    summary_values["n_measurement_filtered"] = int((timeline_mask & measurement_mask).sum())
    summary_values["n_cohort"] = int(cohort_mask.sum())
    summary = pd.Series(summary_values)
    print(summary.to_string())

    selected_columns = []
    if id_column:
        selected_columns.append(id_column)
    selected_columns.extend(condition_selected_columns)
    selected_columns.extend(birth_year_columns)
    selected_columns.extend(recruitment_date_columns)
    selected_columns.extend(recruitment_age_columns)
    selected_columns.extend(measurement_value_columns)
    selected_columns.extend(measurement_date_columns)
    selected_columns.extend(measurement_support_columns)
    selected_columns = list(dict.fromkeys(selected_columns))

    cohort = df.loc[cohort_mask, selected_columns].copy()
    for label_slug, condition_date, condition_age in condition_outputs:
        if condition_date.notna().any():
            cohort[f"{label_slug}_date"] = condition_date.loc[cohort_mask]
        if condition_age.notna().any():
            cohort[f"{label_slug}_age"] = condition_age.loc[cohort_mask]
    if MEASUREMENT_CONSTRAINT_VALUE is not None:
        cohort["measurement_value"] = measurement_value.loc[cohort_mask]
else:
    if SEQUENCE_REQUIRED:
        if procedure_date.notna().any() and primary_diagnosis_date.notna().any():
            sequence_mask = (
                procedure_date.notna()
                & primary_diagnosis_date.notna()
                & procedure_date.ge(primary_diagnosis_date)
            )
        else:
            sequence_mask = pd.Series(False, index=df.index)
            print("Warning: sequence requested but no usable procedure or diagnosis dates were configured.")
    else:
        sequence_mask = pd.Series(True, index=df.index)

    cohort_mask = diagnosis_case & age_mask & measurement_mask & procedure_case & sequence_mask

    summary = pd.Series(
        {
            "n_total": len(df),
            "n_diagnosis_case": int(diagnosis_case.sum()),
            "n_age_filtered": int((diagnosis_case & age_mask).sum()),
            "n_measurement_filtered": int((diagnosis_case & age_mask & measurement_mask).sum()),
            "n_procedure_case": int(procedure_case.sum()),
            "n_cohort": int(cohort_mask.sum()),
        }
    )
    print(summary.to_string())

    selected_columns = []
    if id_column:
        selected_columns.append(id_column)
    selected_columns.extend(diagnosis_code_columns)
    selected_columns.extend(diagnosis_date_columns)
    selected_columns.extend(diagnosis_specific_date_columns)
    selected_columns.extend(diagnosis_year_columns)
    selected_columns.extend(diagnosis_age_columns)
    selected_columns.extend(birth_year_columns)
    selected_columns.extend(recruitment_date_columns)
    selected_columns.extend(recruitment_age_columns)
    selected_columns.extend(measurement_value_columns)
    selected_columns.extend(measurement_date_columns)
    selected_columns.extend(measurement_support_columns)
    selected_columns.extend(procedure_code_columns)
    selected_columns.extend(procedure_date_columns)
    selected_columns = list(dict.fromkeys(selected_columns))

    cohort = df.loc[cohort_mask, selected_columns].copy()
    cohort["age_at_diagnosis"] = age_at_diagnosis.loc[cohort_mask]
    if MEASUREMENT_CONSTRAINT_VALUE is not None:
        cohort["measurement_value"] = measurement_value.loc[cohort_mask]

output_dir = Path("results")
output_dir.mkdir(parents=True, exist_ok=True)
cohort.to_parquet(output_dir / "people_with_rheumatoid_arthritis_diagnosed_before_age_50_who_later_requi_cohort.parquet", index=False)
summary.to_json(output_dir / "people_with_rheumatoid_arthritis_diagnosed_before_age_50_who_later_requi_summary.json", indent=2)

print("Saved:", output_dir / "people_with_rheumatoid_arthritis_diagnosed_before_age_50_who_later_requi_cohort.parquet")
print("Selected columns:", selected_columns)

# Procedure codes were pre-populated from the retrieved UKB context; validate against your final phenotype spec.
