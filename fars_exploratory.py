from read_csv import load_current_data
import pandas as pd


def main():
    
    accident_df, vpic_decode_df, vehicle_df, person_df = load_current_data()
    print(vehicle_df.info())
    print(accident_df.info())
    print(person_df.info())

    # ---------------------------------------------------------
    # Question 1 - out of everyone who lost their life, how many were wearing their seatbelt?
    # ---------------------------------------------------------

    print(person_df['REST_USE'].unique()) 

    print(person_df[ (person_df['REST_USE'] == 20) & (person_df['PER_TYP'].isin(1,2)) & (person_df['INJ_SEV']==4) ])
    






    # ---------------------------------------------------------
    # Step 1 - we extract all Front-to-Front crashes only
    # ---------------------------------------------------------
    # accident_df = accident_df[accident_df["MAN_COLL"]==2]                  # encoded value of 2 means Front-to-Front



if __name__ == "__main__":
    main()