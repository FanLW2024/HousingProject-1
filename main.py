# Lir-Wan Fan
# Purpose: (1) Read a set of sample data, including 3 CSV files (housing-info.csv, income-info.csv, and zip-city-county-state.csv), (2) clean the data, and (3) push that cleaned data into a SQL database

#### pip install pandas
#### pip install pandas numpy
#### pip install pymysql
# Importing pandas for data manipulation
import pandas as pd

# From files.py, importing the file path for further operation.
from files import housingFile, incomeFile, zipFile

# Importing mysql info
import pymysql.cursors

# Containing your database credentials
from cred import *

######################################################################
# (1) Read a set of sample data, including 3 CSV files (housing-info.csv, income-info.csv, and zip-city-county-state.csv)

# Reading data from 3 CSV files importing into Pandas DataFrames
print("Beginning import")

file1_housing = pd.read_csv(housingFile)
file2_income = pd.read_csv(incomeFile)
file3_zip = pd.read_csv(zipFile)

######################################################################
# (2) clean the data

# Removing 'guid' column from all three datasets
file1_housing = file1_housing.drop(columns=['guid'], errors='ignore')
file2_income = file2_income.drop(columns=['guid'], errors='ignore')
file3_zip = file3_zip.drop(columns=['guid'], errors='ignore')

# Function to clean multiple columns by replacing corrupt data
# with specified ranges of random numbers

# Importing numpy for numerical operations
import numpy as np

# importing regular expressions
import re

def cleanRandom(fileNum, column, randLeast, randMost):
    for data in fileNum[column]:
        # Generating a random number (randLeast inclusive, randMost exclusive)
        clean = np.random.randint(randLeast, randMost)

        # For each corrupt data, replacing with the random number
        cleanData = re.sub(r"[A-Z]{4}$", f'{clean}', data)

        # Replacing old column with new column of clean data
        fileNum[column] = fileNum[column].replace(f"{data}", f"{cleanData}")

# file1_housing – Housing File data ###############################
print("Cleaning Housing File data")

# Remaining columns - cleaning by replacing corrupt data with random numbers in specified ranges
cleanRandom(file1_housing, 'housing_median_age', 10, 51)
cleanRandom(file1_housing, 'total_rooms', 1000, 2001)
cleanRandom(file1_housing, 'total_bedrooms', 1000, 2001)
cleanRandom(file1_housing, 'population', 5000, 10001)
cleanRandom(file1_housing, 'households', 500, 2501)
cleanRandom(file1_housing, 'median_house_value', 100000, 250001)

print(f"{len(file1_housing)} records imported into the database")

# file2_income – Income File data #################################
print("Cleaning Income File data")

# median_income column - clean by replacing corrupt data with random numbers in the specified range
cleanRandom(file2_income, 'median_income', 100000, 750001)

print(f"{len(file2_income)} records imported into the database")

# file3_zip – ZIP File data ######################################
print("Cleaning ZIP File data")

# zip_code column - manipulating ZIP code to create a new one by replacing with 1st number of same city state zip + 0000
corruptData = re.compile("^[A-Z]{4}$")

# Creating an empty list for bad ZIP codes and a dictionary for good ZIP codes
badZips = []
goodZips = {}

# Looking through each row for corrupt data, and collecting valid ZIP codes by city and state
for index, row in file3_zip.iterrows():
    if corruptData.match(row['zip_code']):
        # If the Zip code matches the regex, adding indices to the list
        badZips.append(index)

    else:
        # Adding all the good ZIP codes to the dictionary
        cityStateKey = f"{row['city']}{row['state']}"
        goodZips[f"{row.city}{row.state}"] = f"{row.zip_code[0]}0000"

# For each index with corrupt data: fixing the bad ZIP codes
for index in badZips:
    # Getting the city, state and county from this record
    city = file3_zip.iloc[index]['city']
    state = file3_zip.iloc[index]['state']
    county = file3_zip.iloc[index]['county']

    # Combining county and state so we can use them to search our dictionary
    countyStateKey = f"{county}{state}"

    # If a valid ZIP codes exists for the same county-state, using its first two digits followed by '000'
    if countyStateKey in goodZips:
        newZipCode = f"{goodZips[countyStateKey][:3]}00"

    else:
        # Otherwise, generating a new ZIP code using the state's first two digits
        stateZipSample = next((zipCode for key, zipCode in goodZips.items() if key.endswith(state)), None)

        if stateZipSample:
            newZipCode = f"{stateZipSample[:3]}00"

    # Replacing each corrupt ZIP codes with a new clean ZIP codes
    file3_zip.loc[index, 'zip_code'] = newZipCode

file1_housing['zip_code'] = file3_zip['zip_code']
file2_income['zip_code'] = file3_zip['zip_code']

# Replacing zip_code column for all files
file1_housing['zip_code'] = file3_zip['zip_code'].values
file2_income['zip_code'] = file3_zip['zip_code'].values

print(f"{len(file3_zip)} records imported into the database")

# Merging three files (file1_housing, file2_income, file3_zip) to import one database into SQL
# Outer merge since we want all columns with no repeating
mergeFiles12 = pd.merge(file1_housing, file2_income, how='inner', on='zip_code')
mergedAll = pd.merge(mergeFiles12, file3_zip, how='inner', on='zip_code')

######################################################################
# (3) push that cleaned data into a MySQL database
try:
    myConnection = pymysql.connect(
        host=hostname,
        user=username,
        password=password,
        db=database,
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )

# If any error occurs, it will be stored as e, and the error message will be printed, and the program will be exited.
except Exception as e:
    print(f"Error connecting to the database.  Exiting: {e}")
    print()
    exit()

try:
    with myConnection.cursor() as cursor:

        # Our SQL statement using placeholders for each column of data
        sqlInsert = """
                insert
                into
                housing(zip_code, city, state, county, median_age, total_rooms,
                        total_bedrooms, population, households, median_income, median_house_value)
                values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                """
        # Insert values for each row into each column
        for index, row in mergedAll.iterrows():
            cursor.execute(sqlInsert, (f"{row.zip_code}",
                                       f"{row.city}", f"{row.state}", f"{row.county}",
                                       f"{row.housing_median_age}", f"{row.total_rooms}",
                                       f"{row.total_bedrooms}", f"{row.population}",
                                       f"{row.households}", f"{row.median_income}", f"{row.median_house_value}")                                      )

        # Commit the file to the database
        myConnection.commit()

# If there is an exception, show what that is
except Exception as e:
    print(f"An error has occurred.  Exiting: {e}")
    print()

print(f"Import Completed")
print(f"\nBeginning validation\n")

# Beginning Validation - Allow user input for custom values ############
try:
    with myConnection.cursor() as cursor:
        # Asking for user input for totalRooms and zipMedianIncome
        totalRooms = int(input("Total rooms: "))

        # Validation part
        # Our SumBedrooms summation statement
        SumBedrooms = """select
                    sum(total_bedrooms) as bedrooms 
                    from 
                    housing
                    where
                    total_rooms > %s 
                    """

        # User input is value for SumBedrooms
        cursor.execute(SumBedrooms, (totalRooms,))

        # Getting the resulting sum from SQL
        sumResult = cursor.fetchall()
        print(f"For locations with more than {totalRooms} rooms, "
              f"there are a total of {sumResult[0]['bedrooms']} bedrooms.")
        print()

        # Our MedianIncome averaging statement
        zipMedianIncome = int(input("ZIP Code: "))
        MedianIncome = """select
                    format(round(avg(median_income)),0) as zipCode
                    from 
                    housing
                    where
                    zip_code = %s 
                    """

        # User input is value for MedianIncome
        cursor.execute(MedianIncome, (zipMedianIncome,))

        # Getting the resulting avg of median income from sql
        incomeResult = cursor.fetchall()
        print(f"The median household income for ZIP code {zipMedianIncome} is {incomeResult[0]['zipCode']}.")

# If there is an exception, show what that is
except Exception as e:
    print(f"An error has occurred.  Exiting: {e}")
    print()
finally:
    myConnection.close()

print(f"\nProgram exiting.")
