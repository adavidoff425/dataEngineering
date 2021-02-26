import time
import psycopg2
import psycopg2.extras
import argparse
import re
import csv

DBname = "dataeng"
DBuser = "ad35"
DBpwd = "alexison5"
TableName = 'OregonCensus'
Datafile = "Oregon2017.csv"
CreateDB = True
Year = 2017

def row2vals(row):
    for key in row:
        if not row[key]:
            row[key] = 0
        row['County'] = row['County'].replace('\'','')

    ret = f"""
    {Year},                          -- Year
    {row['CensusTract']},            -- CensusTract
    '{row['State']}',                -- State
    '{row['County']}',               -- County
    {row['TotalPop']},               -- TotalPop
    {row['Women']},                  -- Women
    {row['Hispanic']},               -- Hispanic
    {row['White']},                  -- White
    {row['Black']},                  -- Black
    {row['Native']},                 -- Native
    {row['Asian']},                  -- Asian
    {row['Pacific']},                -- Pacific
    {row['Citizen']},                -- Citizen
    {row['Income']},                 -- Income
    {row['IncomeErr']},              -- IncomeErr
    {row['IncomePerCap']},           -- IncomePerCap
    {row['IncomePerCapErr']},        -- IncomePerCapErr
    {row['Poverty']},                -- Poverty
    {row['ChildPoverty']},           -- ChildPoverty
    {row['Professional']},           -- Professional
    {row['Service']},                -- Service
    {row['Office']},                 -- Office
    {row['Construction']},           -- Construction
    {row['Production']},             -- Production
    {row['Drive']},                  -- Drive
    {row['Carpool']},                -- Carpool
    {row['Transit']},                -- Transit
    {row['Walk']},                   -- Walk
    {row['OtherTransp']},            -- OtherTransp
    {row['WorkAtHome']},             -- WorkAtHome
    {row['MeanCommute']},            -- MeanCommute
    {row['Employed']},               -- Employed
    {row['PrivateWork']},            -- PrivateWork
    {row['PublicWork']},             -- PublicWork
    {row['SelfEmployed']},           -- SelfEmployed
    {row['FamilyWork']},             -- FamilyWork
    {row['Unemployment']}            -- Unemployment
    """
    return ret

def initialize():
    global Year

    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--datafile", required=True)
    parser.add_argument("-c", "--createtable", action="store_true")
    parser.add_argument("-y", "--year", default=Year)
    args = parser.parse_args()

    global Datafile
    Datafile = args.datafile
    global CreateDB
    CreateDB = args.createtable
    Year = args.year 

def readdata(fname):
    print(f"readdata: reading from File: {fname}")
    with open(fname, mode="r") as fil:
        dr = csv.DictReader(fil)
        headerRow = next(dr)
        # print(f"Header: {headerRow}")
        rowlist = []
        for row in dr:
            rowlist.append(row)

    return rowlist

def insert_batch(conn, rowlist):

    psycopg2.extras.execute_batch(conn, """
        INSERT INTO staging_census VALUES (
                %(Year)s,
                %(CensusTract)s,
                %(State)s,
                %(County)s,
                %(TotalPop)s, 
                %(Men)s,
                %(Women)s,
                %(Hispanic)s,
                %(White)s,
                %(Black)s,
                %(Native)s,
                %(Asian)s,
                %(Pacific)s,
                %(Citizen)s,
                %(Income)s,
                %(IncomeErr)s,
                %(IncomePerCap)s,
                %(IncomePerCapErr)s,
                %(IncomePerCapErr)s,
                %(Poverty)s,
                %(ChildPoverty)s,
                %(Professional)s,
                %(Service)s, 
                %(Office)s,
                %(Construction)s,
                %(Production)s,
                %(Drive)s,
                %(Carpool)s,
                %(Transit)s,
                %(Walk)s,
                %(OtherTransp),
                %(WorkAtHome)s,
                %(MeanCommute)s,
                %(Employed)s,
                %(PrivateWork)s,
                %(PublicWork)s,
                %(SelfEmployed)s,
                %(FamilyWork)s,
                %(Unemployment)s
            );
        """, rowlist)

def dbconnect():
    connection = psycopg2.connect(
            host="127.0.0.1",
            database=DBname,
            user=DBuser,
            password=DBpwd,
            )
    connection.autocommit = True

    return connection

def createTable(conn):

    with conn.cursor() as cursor:
        cursor.execute(f"""
            DROP TABLE IF EXISTS staging_census;
            CREATE UNLOGGED TABLE staging_census (
                Year                INTEGER,
                CensusTract         NUMERIC,
                State               TEXT,
                County              TEXT,
                TotalPop            INTEGER,
                Men                 INTEGER,
                Women               INTEGER,
                Hispanic            DECIMAL,
                White               DECIMAL,
                Black               DECIMAL,
                Native              DECIMAL,
                Asian               DECIMAL,
                Pacific             DECIMAL,
                Citizen             DECIMAL,
                Income              DECIMAL,
                IncomeErr           DECIMAL,
                IncomePerCap        DECIMAL,
                IncomePerCapErr     DECIMAL,
                Poverty             DECIMAL,
                ChildPoverty        DECIMAL,
                Professional        DECIMAL,
                Service             DECIMAL,
                Office              DECIMAL,
                Construction        DECIMAL,
                Production          DECIMAL,
                Drive               DECIMAL,
                Carpool             DECIMAL,
                Transit             DECIMAL,
                Walk                DECIMAL,
                OtherTransp         DECIMAL,
                WorkAtHome          DECIMAL,
                MeanCommute         DECIMAL,
                Employed            INTEGER,
                PrivateWork         DECIMAL,
                PublicWork          DECIMAL,
                SelfEmployed        DECIMAL,
                FamilyWork          DECIMAL,
                Unemployment        DECIMAL
            );
            """)
        print(f"Create unlogged")

def load(conn):

    with conn.cursor() as cursor:
        start = time.perf_counter()


        rlis = readdata(Datafile)
        data = [{
            'Year': Year,
            **row,
        } for row in rlis]
        print(rlis[1])
        print(data[1])
        psycopg2.extras.execute_batch(cursor, """
            INSERT INTO staging_census VALUES (
                    %(Year)s,
                    %(CensusTract)s,
                    %(State)s,
                    %(County)s,
                    %(TotalPop)s, 
                    %(Men)s,
                    %(Women)s,
                    %(Hispanic)s,
                    %(White)s,
                    %(Black)s,
                    %(Native)s,
                    %(Asian)s,
                    %(Pacific)s,
                    %(Citizen)s,
                    %(Income)s,
                    %(IncomeErr)s,
                    %(IncomePerCap)s,
                    %(IncomePerCapErr)s,
                    %(Poverty)s,
                    %(ChildPoverty)s,
                    %(Professional)s,
                    %(Service)s, 
                    %(Office)s,
                    %(Construction)s,
                    %(Production)s,
                    %(Drive)s,
                    %(Carpool)s,
                    %(Transit)s,
                    %(Walk)s,
                    %(OtherTransp)s,
                    %(WorkAtHome)s,
                    %(MeanCommute)s,
                    %(Employed)s,
                    %(PrivateWork)s,
                    %(PublicWork)s,
                    %(SelfEmployed)s,
                    %(FamilyWork)s,
                    %(Unemployment)s
                );
            """, data) 

        cursor.execute(f"""
            DROP TABLE IF EXISTS {TableName};
            CREATE TABLE {TableName} AS TABLE staging_census;
            ALTER TABLE {TableName} ADD PRIMARY KEY (Year, CensusTract);
            CREATE INDEX idx_{TableName}_State ON {TableName}(State);
        """)
        elapsed = time.perf_counter() - start
        print(f"Finished Loading. Elapsed Time: {elapsed:0.4} seconds")

def main():
    initialize()
    conn = dbconnect()

    if CreateDB:
        createTable(conn)

    load(conn)

if __name__ == "__main__":
    main()

