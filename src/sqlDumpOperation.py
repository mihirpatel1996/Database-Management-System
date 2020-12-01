import json
import re
import os

from src.log_manager import *

def createDump(user, dbName):
    try:
        dumpFile = open("../DB/" + dbName + "/" + "sqlDump.txt", "a+")
        dumpFile.truncate(0)
        existing_tables = []
        for file in os.listdir("../DB/" + dbName):
            if file.endswith(".json"):
                existing_tables.append(file[:len(file)-5])
        dumpFile.write("CREATE DATABASE " + dbName + ";\n")
        for table in existing_tables:
            with open("../DB/" + dbName + "/" + table + ".json", "r") as tablejson:
                create_query = ""
                line = tablejson.readline()
                json_data = json.loads(line)
                create_query += "CREATE TABLE " + json_data['table_name'] + " ("

                cols_data = json_data['columns']
                col_index = 0
                for col in cols_data:
                    if col_index > 0:
                        create_query += ", "
                    create_query += (col['col_name'] + " " + col['col_type'])
                    col_index += 1
                    if col['primary_key'] == 'true':
                        create_query += " PRIMARY KEY"

                if "foreign_info" in json_data:
                    foreign_info = json_data['foreign_info']
                    create_query += (", FOREIGN KEY(" + foreign_info['column_name'] + ")" + " REFERENCES " + foreign_info['referenced_table'] + "(" + foreign_info['referenced_column'] + ')')

                create_query += ");"
                dumpFile.write(create_query + "\n")
        write_log("SQL DUMP created for database:" + dbName)
        dumpFile.flush()
        dumpFile.close()
    except:
        print("ERD cannot be created")

def sqlDumpQuery(query, user):
    regex = r'SQLDUMP (.*);'
    sqlDumpRegex = re.compile(regex)
    if re.match(regex, query):
        data = sqlDumpRegex.search(query)
        dbName = data.groups()[0]
        dbList = [item for item in os.listdir("../DB") if os.path.isdir(os.path.join("../DB", item))]
        if dbName in dbList:
            createDump(user, dbName)
        else:
            print("Database", dbName, "does not exists")
        pass
    else:
        print("SQLDUMP query not in proper format")
