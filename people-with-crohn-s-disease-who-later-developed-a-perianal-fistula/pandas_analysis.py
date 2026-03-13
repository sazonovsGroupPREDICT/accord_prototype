from pathlib import Path
import re

import pandas as pd

DATA_PATH = Path("data/people_with_crohn_s_disease_who_later_developed_a_perianal_fistula.tsv.gz")
DELIMITER = "\t"
ID_COLUMN_CANDIDATES = ["eid", "participant.eid", "f.eid", "p.eid"]

FIELD_IDS = [34, 53, 21003, 21022, 26229, 26230, 41202, 131626]
DIAGNOSIS_FIELD_IDS = [41202]
DIAGNOSIS_ANCHOR_FIELD_IDS = [26229, 26230, 41202, 131626]
DIAGNOSIS_DATE_FIELD_IDS = [131626]
DIAGNOSIS_SPECIFIC_DATE_FIELD_IDS = []
DIAGNOSIS_YEAR_FIELD_IDS = []
DIAGNOSIS_AGE_FIELD_IDS = []
BIRTH_YEAR_FIELD_IDS = [34]
RECRUITMENT_DATE_FIELD_IDS = [53]
RECRUITMENT_AGE_FIELD_IDS = [21003, 21022]
MEASUREMENT_FIELD_IDS = []
MEASUREMENT_DATE_FIELD_IDS = []
MEASUREMENT_SUPPORT_FIELD_IDS = []
PROCEDURE_FIELD_IDS = []
PROCEDURE_ANCHOR_FIELD_IDS = []
PROCEDURE_DATE_FIELD_IDS = []

DIAGNOSIS_EXACT_CODES = []
DIAGNOSIS_PREFIXES = ['K50', 'K500', 'K501', 'K508', 'K509', 'M074', 'M0743', 'M0744', 'M0748']
PROCEDURE_EXACT_CODES = []
PROCEDURE_PREFIXES = []

AGE_LIMIT = None
SEQUENCE_REQUIRED = True
MEASUREMENT_CONSTRAINT_LABEL = None
MEASUREMENT_CONSTRAINT_OPERATOR = None
MEASUREMENT_CONSTRAINT_VALUE = None
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
cohort.to_parquet(output_dir / "people_with_crohn_s_disease_who_later_developed_a_perianal_fistula_cohort.parquet", index=False)
summary.to_json(output_dir / "people_with_crohn_s_disease_who_later_developed_a_perianal_fistula_summary.json", indent=2)

print("Saved:", output_dir / "people_with_crohn_s_disease_who_later_developed_a_perianal_fistula_cohort.parquet")
print("Selected columns:", selected_columns)

# Add validated OPCS or operation codes for the procedure signal before treating this as final.
