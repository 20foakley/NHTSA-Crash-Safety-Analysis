import os
import glob
import pandas as pd
from collections import defaultdict
from dotenv import load_dotenv
import re

load_dotenv()

base_dir = os.getenv("BASE_DIR")
accident_filename = os.getenv("ACCIDENT_FILENAME")
vpic_decode_filename = os.getenv("VPIC_DECODE_FILENAME")
vehicle_filename = os.getenv("VEHICLE_FILENAME")
person_filename = os.getenv("PERSON_FILENAME")

# -------------------------------
# Finding Files
# -------------------------------

def find_csv(filename):
    '''Return filenames of CSVs
       We already filtered by year in get_fars_crashes.py
       so no year filtering is needed here.   
    '''
    patterns = [
        os.path.join(base_dir, "*",f"{filename}.csv"),
        os.path.join(base_dir, "*","*",f"{filename}.csv")
    ]
    files = []
    for p in patterns:
        files.extend(glob.glob(p))
    files = sorted(files)
    return files

# -------------------------------
# CSV Reading Helpers
# -------------------------------

def load_csvs_by_year(filename,years=None):
    files = find_csv(filename)
    return group_by_years(files,years)
    
def load_csv(f):
    try:
        return pd.read_csv(f,encoding="utf-8-sig",low_memory=False)
    except UnicodeDecodeError:
        return pd.read_csv(f,encoding="latin-1",low_memory=False)

def extract_year(filename):
    match = re.search(r"\d{4}",filename).group()
    if match:
        return int(match)
    return None

def group_by_years(files,years):
    dfs_by_year={}
    for f in files:
        year = extract_year(f)
        if year is None:
            continue
        if years is not None and year not in years:
            continue
        dfs_by_year[year]=load_csv(f) 
    return dfs_by_year

# -------------------------------
# Data Combination
# -------------------------------

def concat_df(dfs_by_year):
    dfs_to_concat = []
    for y in sorted(dfs_by_year.keys()):
        df = dfs_by_year[y].copy()
        df["YEAR"]=y 
        dfs_to_concat.append(df)
    return pd.concat(dfs_to_concat,ignore_index=True)

# -------------------------------
# Final Loader
# -------------------------------

def load_current_data(years=[2021, 2022, 2023]):

    accident_by_year = load_csvs_by_year(accident_filename, years)
    vpic_decode_by_year = load_csvs_by_year(vpic_decode_filename, years)
    vehicle_by_year = load_csvs_by_year(vehicle_filename, years)
    person_by_year = load_csvs_by_year(person_filename, years)

    accident_df = concat_df(accident_by_year)
    vpic_decode_df = concat_df(vpic_decode_by_year)
    vehicle_df = concat_df(vehicle_by_year)
    person_df = concat_df(person_by_year)

    accident_keep_cols = [
        'STATE','STATENAME','ST_CASE','YEAR','DAY_WEEK','DAY_WEEKNAME','HOUR','HOURNAME',
        'RUR_URB','RUR_URBNAME','FUNC_SYS','FUNC_SYSNAME','PEDS','PERSONS','HARM_EV',
        'HARM_EVNAME','MAN_COLL','MAN_COLLNAME','FATALS'
    ]

    vpic_decode_keep_cols = [
        'STATE','STATENAME','ST_CASE','YEAR','VEH_NO','VEHICLETYPEID','VEHICLETYPE',
        'MAKEID','MAKE','MODELID','MODEL','MODELYEAR','BODYCLASS',
        'GROSSVEHICLEWEIGHTRATINGFROM','ENGINEBRAKEHP_FROM','ENGINEBRAKEHP_TO'
    ]

    vehicle_keep_cols = [
        'STATE','STATENAME','ST_CASE','YEAR','VEH_NO','VE_FORMS','DR_DRINK','DR_HGT',
        'DR_WGT','NUMOCCS','NUMOCCSNAME','TRAV_SP','ROLLOVER','ROLLOVERNAME',
        'M_HARM','M_HARMNAME','MOD_YEAR','SPEEDREL','SPEEDRELNAME'
    ]

    person_keep_cols = [
        'STATE','STATENAME','ST_CASE','YEAR','AGE','AGENAME','SEX','INJ_SEV','INJ_SEVNAME',
        'SEAT_POS','SEAT_POSNAME','PER_TYP','PER_TYPNAME','REST_USENAME','REST_USE','REST_MISNAME','REST_MIS',
        'AIR_BAGNAME','EJECTION','EJECTIONNAME','DRINKING','DRINKINGNAME',
        'ALC_RES','ALC_RESNAME','DRUGS','DRUGSNAME'
    ]

    accident_df = accident_df[[c for c in accident_keep_cols if c in accident_df.columns]]
    vpic_decode_df = vpic_decode_df[[c for c in vpic_decode_keep_cols if c in vpic_decode_df.columns]]
    vehicle_df = vehicle_df[[c for c in vehicle_keep_cols if c in vehicle_df.columns]]
    person_df = person_df[[c for c in person_keep_cols if c in person_df.columns]]

    return accident_df, vpic_decode_df, vehicle_df, person_df