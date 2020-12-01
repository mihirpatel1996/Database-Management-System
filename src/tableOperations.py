# class User(object):
#     username = ""
import re
import json
import os
import csv

def createTableQuery(dbName, query):
    VALID_COL_TYPES = ["INT", "STRING", "DATE"]
    # query = "ADD USER user1 TO DB1;"
    # query = "CREATE TABLE table2 (id INT PRIMARY KEY, name STRING, FOREIGN KEY(id) REFERENCES table1(id));"
    # query = "CREATE TABLE table1 (id INT PRIMARY KEY, name STRING);"
    # regex = r'ADD USER (.*) TO (.*);
    regex = r"CREATE TABLE (.*) \((.*)\);"
    createTableRegex = re.compile(regex)
    existing_tables = []
    for file in os.listdir("../DB/" + dbName):
        if file.endswith(".json"):
            existing_tables.append(file[:len(file)-5])
    print("existing_tables",existing_tables)
    if re.match(regex, query):
        print("query valid")
        data = createTableRegex.search(query)
        table_name = data.groups()[0]
        # print("group 0 ", data.groups()[0])
        print("match groups: ", data.groups(), type(data.groups()))

        if table_name in existing_tables:
            print("table:", table_name, "already exists")
        else:

            json_obj = {}
            json_obj['table_name'] = table_name

            columns = data.groups()[1].split(",")
            print("columns ", columns, type(columns))
            columns = [col.strip() for col in columns]
            print("columns ", columns, type(columns))
            cols_data = []
            table_columns = []
            for col in columns:
                col_info = {}
                if "FOREIGN KEY" in col:
                    # pass
                    foriegn_info = {}
                    print("foriegn key column")
                    foreign_regex = r'FOREIGN KEY\((.*)\) REFERENCES (.*)\((.*)\)'
                    # foreign_regex = r'FOREIGN KEY(.*) REFERENCES (.*)'
                    foreignKeyRegex = re.compile(foreign_regex)
                    if re.match(foreign_regex, col):
                        foreign_data = foreignKeyRegex.search(col)
                        print("foreign_data", foreign_data.groups())
                        foreign_column = foreign_data.groups()[0]
                        referenced_table = foreign_data.groups()[1]
                        referenced_col = foreign_data.groups()[2]
                        if checkDbAndTableExists(dbName, referenced_table, referenced_col) and foreign_column in table_columns:
                            foriegn_info['column_name'] = foreign_column
                            foriegn_info['referenced_table'] = referenced_table
                            foriegn_info['referenced_column'] = referenced_col
                            json_obj['foreign_info'] = foriegn_info
                            # pass
                        # else:
                        #     pass
                        # pass
                    else:
                        print("Foriegn key syntax not proper")
                else:
                    col_data = col.split(" ")
                    col_name = col_data[0]
                    col_type = col_data[1]
                    if col_type not in VALID_COL_TYPES:
                        print("Column type", col_type, "invalid for column", col_name)
                    else:
                        # print("Column type", col_type, "valid for column", col_name)
                        table_columns.append(col_name)
                        col_info['col_name'] = col_name
                        col_info['col_type'] = col_type
                        cols_data.append(col_info)
                        print("col_info", col_info)
                    if "PRIMARY KEY" in col:
                        col_info['primary_key'] = 'true'
                    else:
                        col_info['primary_key'] = 'false'
            print("cols_data", cols_data)
            print("table_columns", table_columns)
            json_obj['columns'] = cols_data


            json_data = json.dumps(json_obj)
            print("json_data", json_data)

            with open("../DB/" + dbName + "/" + table_name + ".json", "w") as tablejson:
                tablejson.write(json_data)
                tablejson.flush()
            tablejson.close()

            with open("../DB/" + dbName + "/" + table_name + ".csv", "w") as tablecsv:
                wr = csv.writer(tablecsv)
                wr.writerow(table_columns)
                tablecsv.flush()
            tablecsv.close()


    else:
        print("invalid query, ", query)

# with open("../DB/DB1/dbuser.csv", "a+") as userfile:
#     userfile.seek(0)
#     print(userfile.readlines())
#     # userfile.seek(0,2)
#     userfile.write("user1\n")
#     print("after write", userfile.readlines())

def checkDbAndTableExists(dbName, tableName, columnName) -> bool:
    print("in checkDbAndTableExists")
    dbPath = "../DB/" + dbName
    try:
        existing_tables = []
        for file in os.listdir(dbPath):
            if file.endswith(".json"):
                existing_tables.append(file[:len(file) - 5])
        print("existing_tables", existing_tables)
        if tableName in existing_tables:
            columnFound = False
            with open(dbPath + "/" + tableName + ".json", "r") as tablecsv:
                data = tablecsv.readline()
                print("data", data)
                data_json = json.loads(data)
                print("data_json", data_json['columns'])
                for column in data_json['columns']:
                    if column['col_name'] == columnName:
                        if column['primary_key'] == 'true':
                            columnFound = True
                            return True
                        else:
                            print("Column", columnName, "is not primary key in", tableName)
                            return False
                if columnFound:
                    print("Column", columnName, "does not exists in", tableName)
                    return False
            #     if columnName in cols:
            #         return True
            #     else:
            #         print("Column", columnName, "does not exists in", tableName)
            #         return False
            # pass
        else:
            print("Table", tableName, "does not exists in", dbName)
            return False
        #     pass
        # print("5")
    except:
        print("db does not exists")
        return False
    # return True

# checkDbAndTableExists("DB1", "table2", "id")
# start()

def deleteTable(dbName, tableName) -> bool:
    dbPath = "../DB/" + dbName
    # safe_to_delete = False
    try:
        existing_tables = []
        for file in os.listdir(dbPath):
            if file.endswith(".json"):
                existing_tables.append(file[:len(file) - 5])
        # print("existing_tables", existing_tables)
        if tableName in existing_tables:
            for table in existing_tables:
                with open(dbPath + "/" + table + ".json", 'r') as tablejson:
                    json_data = json.loads(tablejson.readline())
                    if "foreign_info" in json_data:
                        if json_data['foreign_info']['referenced_table'] == tableName:
                            print(tableName, "has a foriegn key reference in", table, "table")
                            return False
                        else:
                            print("table", tableName, "safe to delete")
                            return True
        #                 return True
        else:
            print("Table", tableName, "does not exists")
            return False
    except:
        print("db", dbName, "does not exists")
        return False
    # return True
# dbname = "DB1"
# table_name = "table2"
# if deleteTable(dbname, table_name):
#     os.remove("../DB/" + dbname + "/" + table_name + ".csv")
#     os.remove("../DB/" + dbname + "/" + table_name + ".json")

def dropDb(dbName):
    dbPath = "../DB/" + dbName
    safe_to_delete = True
    try:
        existing_tables = []
        for file in os.listdir(dbPath):
            if file.endswith(".json"):
                existing_tables.append(file[:len(file) - 5])
        for table in existing_tables:
            if not deleteTable(dbName, table):
               safe_to_delete = False
        print("safe to drop db", dbName, safe_to_delete)
        if safe_to_delete:
            os.removedirs(dbPath)
    except:
        print("db", dbName, "does not exists")

# dropDb("DB1")
createTableQuery("DB1", "CREATE TABLE table1 (id INT PRIMARY KEY, name STRING);")
createTableQuery("DB1", "CREATE TABLE table2 (id INT PRIMARY KEY, name STRING, FOREIGN KEY(id) REFERENCES table1(id));")
