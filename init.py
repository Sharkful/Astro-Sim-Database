import os
import random
import pandas as pd
import numpy as np
import mysql.connector

def create_database_and_table():

    db = mysql.connector.connect(
        host='127.0.0.1',
        user='root',
        password='pswd'
    )

    cursor = db.cursor()

    cursor.execute("CREATE DATABASE IF NOT EXISTS AstroSim")
    cursor.execute("USE AstroSim")

    sql_schema = """
    CREATE TABLE IF NOT EXISTS Mergers (
        root_SubHaloID BIGINT,
        merger_ratio DOUBLE,
        child_SnapNum SMALLINT,
        child_SubhaloID BIGINT,
        child_SubhaloIDRaw BIGINT,
        child_SubhaloMass FLOAT,
        primary_SnapNum SMALLINT,
        primary_SubhaloID BIGINT,
        primary_SubhaloIDRaw BIGINT,
        primary_SubhaloMass FLOAT,
        secondary_SnapNum SMALLINT,
        secondary_SubhaloID BIGINT,
        secondary_SubhaloIDRaw BIGINT,
        secondary_SubhaloMass FLOAT,
        image_path VARCHAR(255),
        PRIMARY KEY (child_SubhaloID, child_SnapNum)
    );
    """
    cursor.execute(sql_schema)
    sql_schema = """
    CREATE TABLE IF NOT EXISTS Subhalos (
        SubhaloBHMass FLOAT,
        SubhaloBHMdot FLOAT,
        SubhaloFlag TINYINT(1),
        SubhaloGasMetallicity FLOAT,
        SubhaloGasMetallicityHalfRad FLOAT,
        SubhaloGasMetallicityMaxRad FLOAT,
        SubhaloGasMetallicitySfr FLOAT,
        SubhaloGasMetallicitySfrWeighted FLOAT,
        SubhaloGrNr BIGINT,
        SubhaloHalfmassRad FLOAT,
        SubhaloIDMostbound BIGINT,
        SubhaloLen BIGINT,
        SubhaloMass FLOAT,
        SubhaloMassInHalfRad FLOAT,
        SubhaloMassInMaxRad FLOAT,
        SubhaloMassInRad FLOAT,
        SubhaloParent BIGINT,
        SubhaloSFR FLOAT,
        SubhaloSFRinHalfRad FLOAT,
        SubhaloSFRinMaxRad FLOAT,
        SubhaloSFRinRad FLOAT,
        SubhaloStarMetallicity FLOAT,
        SubhaloStarMetallicityHalfRad FLOAT,
        SubhaloStarMetallicityMaxRad FLOAT,
        SubhaloStellarPhotometricsMassInRad FLOAT,
        SubhaloStellarPhotometricsRad FLOAT,
        SubhaloVelDisp FLOAT,
        SubhaloVmax FLOAT,
        SubhaloVmaxRad FLOAT,
        SubhaloWindMass FLOAT,
        SubhaloIDRaw BIGINT,
        SubhaloBfldDisk FLOAT,
        SubhaloBfldHalo FLOAT,
        PRIMARY KEY (SubhaloIDRaw)
    );
    """
    cursor.execute(sql_schema)

    cursor.close()
    db.close()

def insert_merger_data(df, connection):
    cursor = connection.cursor()

    # Check if the table is already populated
    cursor.execute("SELECT 1 FROM mergers LIMIT 1")
    if cursor.fetchone():
        print("Table is already populated. Skipping insertion.")
        return

    insert_query = """
    INSERT INTO Mergers (root_SubHaloID, merger_ratio, child_SnapNum, 
                          child_SubhaloID, child_SubhaloIDRaw, child_SubhaloMass, primary_SnapNum, 
                          primary_SubhaloID, primary_SubhaloIDRaw, primary_SubhaloMass, secondary_SnapNum, 
                          secondary_SubhaloID, secondary_SubhaloIDRaw, secondary_SubhaloMass, image_path) 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    
    for i, row in df.iterrows():
        image_file = f"Images\\{format(random.randint(0, 10), '04')}_model.png"
        data = (
            int(row['root-SubHaloID']),
            float(row['merger-ratio']),  # Casting to float
            int(row['child-SnapNum']),
            int(row['child-SubhaloID']),
            int(row['child-SubhaloIDRaw']),
            float(row['child-SubhaloMass']),  # Casting to float
            int(row['primary-SnapNum']),
            int(row['primary-SubhaloID']),
            int(row['primary-SubhaloIDRaw']),
            float(row['primary-SubhaloMass']),  # Casting to float
            int(row['secondary-SnapNum']),
            int(row['secondary-SubhaloID']),
            int(row['secondary-SubhaloIDRaw']),
            float(row['secondary-SubhaloMass']),  # Casting to float
            image_file
        )
        print("Loading Row:", i, "of", len(df))
        cursor.execute(insert_query, data)

    connection.commit()
    cursor.close()

def insert_subhalo_data(df, connection):
    cursor = connection.cursor()

    # Check if the table is already populated
    cursor.execute("SELECT 1 FROM Subhalos LIMIT 1")
    if cursor.fetchone():
        print("Table is already populated. Skipping insertion.")
        return

    insert_query = """
    INSERT INTO Subhalos (SubhaloBHMass, SubhaloBHMdot, SubhaloFlag, 
                          SubhaloGasMetallicity, SubhaloGasMetallicityHalfRad, 
                          SubhaloGasMetallicityMaxRad, SubhaloGasMetallicitySfr, 
                          SubhaloGasMetallicitySfrWeighted, SubhaloGrNr, SubhaloHalfmassRad, 
                          SubhaloIDMostbound, SubhaloLen, SubhaloMass, 
                          SubhaloMassInHalfRad, SubhaloMassInMaxRad, SubhaloMassInRad, 
                          SubhaloParent, SubhaloSFR, SubhaloSFRinHalfRad, SubhaloSFRinMaxRad, 
                          SubhaloSFRinRad, SubhaloStarMetallicity, SubhaloStarMetallicityHalfRad, 
                          SubhaloStarMetallicityMaxRad, SubhaloStellarPhotometricsMassInRad, 
                          SubhaloStellarPhotometricsRad, SubhaloVelDisp, SubhaloVmax, 
                          SubhaloVmaxRad, SubhaloWindMass, SubhaloIDRaw, 
                          SubhaloBfldDisk, SubhaloBfldHalo) 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
            %s, %s, %s)
    """ 

    
    for i, row in df.iterrows():
        data = (
            float(row['SubhaloBHMass']),
            float(row['SubhaloBHMdot']),
            int(row['SubhaloFlag']),
            float(row['SubhaloGasMetallicity']),
            float(row['SubhaloGasMetallicityHalfRad']),
            float(row['SubhaloGasMetallicityMaxRad']),
            float(row['SubhaloGasMetallicitySfr']),
            float(row['SubhaloGasMetallicitySfrWeighted']),
            int(row['SubhaloGrNr']),
            float(row['SubhaloHalfmassRad']),
            int(row['SubhaloIDMostbound']),
            int(row['SubhaloLen']),
            float(row['SubhaloMass']),
            float(row['SubhaloMassInHalfRad']),
            float(row['SubhaloMassInMaxRad']),
            float(row['SubhaloMassInRad']),
            int(row['SubhaloParent']),
            float(row['SubhaloSFR']),
            float(row['SubhaloSFRinHalfRad']),
            float(row['SubhaloSFRinMaxRad']),
            float(row['SubhaloSFRinRad']),
            float(row['SubhaloStarMetallicity']),
            float(row['SubhaloStarMetallicityHalfRad']),
            float(row['SubhaloStarMetallicityMaxRad']),
            float(row['SubhaloStellarPhotometricsMassInRad']),
            float(row['SubhaloStellarPhotometricsRad']),
            float(row['SubhaloVelDisp']),
            float(row['SubhaloVmax']),
            float(row['SubhaloVmaxRad']),
            float(row['SubhaloWindMass']),
            int(row['SubhaloIDRaw']),
            float(row['SubhaloBfldDisk']),
            float(row['SubhaloBfldHalo'])
        )
        print("Loading Row:", i, "of", len(df))
        cursor.execute(insert_query, data)

    connection.commit()
    cursor.close()

def verify_csv_integrity(file_path, expected_dtypes):
    """
    Verifies the integrity of a CSV file.

    Args:
    file_path (str): The path to the CSV file.
    expected_dtypes (dict): A dictionary mapping column names to their expected data types.

    Returns:
    bool: True if the file passes the checks, False otherwise.
    """
    try:
        # Read the CSV file
        df = pd.read_csv(file_path)

        # Check for missing values
        if df.isnull().values.any():
            print("Error: CSV file contains missing values.")
            return False

        # Check data types
        for column, expected_dtype in expected_dtypes.items():
            if column in df.columns:
                if not df[column].map(type).eq(expected_dtype).all():
                    print(f"Error: Column '{column}' contains unexpected data types.")
                    return False
            else:
                print(f"Error: Column '{column}' does not exist in the CSV file.")
                return False

        print("CSV file passed all checks.")
        return True

    except Exception as e:
        print(f"An error occurred: {e}")
        return False

def main():
    # Create database and table
    create_database_and_table()

    # Establish a connection to the MySQL server
    db = mysql.connector.connect(
        host='127.0.0.1',
        user='root',
        password='pswd',
        database="AstroSim"
    )

    # Load CSV file
    csv_file = "RawData/TNG100-1-merger-events-10 (1).csv"  # Replace with your CSV file path
    df = pd.read_csv(csv_file)

    expected_dtypes = {
        'root-SubHaloID': int,
        'merger-ratio': float,
        'child-SnapNum': int,
        'child-SubhaloID': int,
        'child-SubhaloIDRaw': int,
        'child-SubhaloMass': float,
        'primary-SnapNum': int,
        'primary-SubhaloID': int,
        'primary-SubhaloIDRaw': int,
        'primary-SubhaloMass': float,
        'secondary-SnapNum': int,
        'secondary-SubhaloID': int,
        'secondary-SubhaloIDRaw': int,
        'secondary-SubhaloMass': float
    }
    verify_csv_integrity(csv_file, expected_dtypes)

    # Insert data into the database
    insert_merger_data(df, db)

    # Insert the subhalo data
    csv_file = "RawData/TNG100-1-subhalo-data-10.csv"
    df = pd.read_csv(csv_file)
    df = df.drop_duplicates(subset='SubhaloIDRaw')
    print(f"Num rows: {len(df)}, Num unique RAW Ids: {df['SubhaloIDRaw'].nunique()}")
    insert_subhalo_data(df, db)

    # Close the connection
    db.close()

if __name__ == "__main__":
    main()