import json
import re
import os

from src.log_manager import *

def createErd(user, dbName):
    try:
        erdFile = open("../DB/" + dbName + "/" + "erd.txt", "a+")
        erdFile.truncate(0)
        existing_tables = []
        for file in os.listdir("../DB/" + dbName):
            if file.endswith(".json"):
                existing_tables.append(file[:len(file)-5])
        erdFile.write("DATABASE NAME: " + dbName + "\n")
        for table in existing_tables:
            with open("../DB/" + dbName + "/" + table + ".json", "r") as tablejson:
                create_query = ""
                line = tablejson.readline()
                json_data = json.loads(line)
                erdFile.write("\nTable Name " + json_data['table_name'] + "\n")

                cols_data = json_data['columns']
                col_index = 0
                for col in cols_data:

                    erdFile.write("Column Name: " + col['col_name'] + " Type: " + col['col_type'])
                    # col_index += 1
                    if col['primary_key'] == 'true':
                        erdFile.write(" PRIMARY KEY")
                    erdFile.write("\n")

                if "foreign_info" in json_data:
                    foreign_info = json_data['foreign_info']
                    erdFile.write("FOREIGN KEY " + foreign_info['column_name'] + " REFERENCES " + foreign_info['referenced_table'] + " " + foreign_info['referenced_column'] + "\n")
                    for col in cols_data:
                        if foreign_info['column_name'] == col['col_name']:
                            if col['primary_key'] == 'true':
                                erdFile.write("Relationship: One to One\n")
                            else:
                                erdFile.write("Relationship: One to Many\n")

                # create_query += ");"
                # erdFile.write(create_query + "\n")
        # write_log("SQL DUMP created for database:" + dbName)
        erdFile.flush()
        erdFile.close()
    except:
        print("ERD cannot be created")

def erdQuery(query, user):
    regex = r'CREATE ERD (.*);'
    sqlDumpRegex = re.compile(regex)
    if re.match(regex, query):
        data = sqlDumpRegex.search(query)
        dbName = data.groups()[0]
        dbList = [item for item in os.listdir("../DB") if os.path.isdir(os.path.join("../DB", item))]
        if dbName in dbList:
            createErd(user, dbName)
        else:
            print("Database", dbName, "does not exists")
        pass
    else:
        print("CREATE ERD query not in proper format")
