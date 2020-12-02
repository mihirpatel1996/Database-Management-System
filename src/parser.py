import csv
import re
import pandas as pd
import os.path
from os import path


class parsor:

    # INSERT INTO Customers (CustomerName, ContactName, Address, City, PostalCode, Country) VALUES ('Cardinal', 'Tom B. Erichsen', 'Skagen 21', 'Stavanger', '4006', 'Norway');
    def insert(self, query, db, user):

        table_name_pattern1 = r"^INSERT\sINTO\s[\w]+\s\([\w\s,]+\)\sVALUES\s\([\w\W]+\);$"
        table_name_pattern2 = r"^INSERT\sINTO\s[\w]+\s\([\w,]+\)\sVALUES\s\([\w\W]+\);$"
        table_name_pattern3 = r"^insert\sinto\s[\w]+\s\([\w\s,]+\)\svalues\s\([\w\W]+\);$"
        db_name = db
        path = "../DB/"+db_name+"/"

        # findall matches gives matched query
        # table_syntax_check = re.findall(table_name_pattern, query)
        table_syntax_check1 = re.search(table_name_pattern1, query)
        table_syntax_check2 = re.search(table_name_pattern2, query)
        table_syntax_check3 = re.search(table_name_pattern3, query)

        if(table_syntax_check1 == None and table_syntax_check2 == None and table_syntax_check3 == None):
            print("Syntax Invalid")
            return
        else:
            print("syntax matched")

        # check whether table exists or not
        parsed_for_table_name = query.split(" ")
        table_name = parsed_for_table_name[2]
        full_path = path+table_name + ".csv"

        if(os.path.exists(full_path) == False):
            print("Table does not exist")
            return

        parsed_query1 = query.split(",")
        # print("parsed_query1:", parsed_query1)

        # syntax for insert length checked
        if(len(parsed_query1) <= 6):
            print("Syntax Invalid")
            return

        # to find values of column name
        parentheses_count = 0
        column_name_flag = False
        column_names = []

        for i in range(0, len(parsed_query1)):
            # print("parsed_query value:", parsed_query1[i])

            if(')' in parsed_query1[i]):
                # print(") matched:", parsed_query1[i])
                parsed_array = parsed_query1[i].split(" ")
                # print("parsed_array:", parsed_array)
                for i in range(0, len(parsed_array)):
                    if(')' in parsed_array[i]):
                        replaced_string = parsed_array[i].replace(")", "")
                        column_names.append(replaced_string)
                        column_name_flag = False
                break

            if('(' in parsed_query1[i]):
                column_name_flag = True
                # print("( matched:", parsed_query1[i])
                parsed_array = parsed_query1[i].split(" ")
                replaced_string = parsed_array[len(
                    parsed_array)-1].replace("(", "")
                replaced_string = replaced_string.strip()
                column_names.append(replaced_string)
                continue

            if(column_name_flag == True):
                replaced_string = parsed_query1[i].replace(",", "")
                replaced_string = replaced_string.strip()
                column_names.append(replaced_string)

        # getting values from query
        values = []
        parsed_values = query.split("'")

        for i in range(0, len(parsed_values)):
            if("," in parsed_values[i]):
                continue
            if(")" in parsed_values[i]):
                continue
            else:
                values.append(parsed_values[i])

        # to check if column names and values match
        if(len(column_names) != len(values)):
            print("Invalid Syntax")
            return

        d = {column_names[i]: values[i]
             for i in range(0, len(column_names), 1)}

        # matching columns of table and query
        df = pd.read_csv(full_path)

        for i in range(0, len(column_names)):
            if(column_names[i] not in df.columns):
                print("columns not matched")
                return

        column_name = column_names[0]
        column_value = values[0]

        # checking for duplicate record
        duplicate_df = df.loc[df[column_name] == column_value]
        if not duplicate_df.empty:
            print("Reccord Exist")
            return
        # writing dictionary in file
        with open(r'../DB/DB1/table1.csv', 'a', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=column_names)
            writer.writerow(d)
            print("Row written to file")
        return

    # ----------------------------------------------------------------------select-------------------------------
    # select query function
    def select(self, query, db, user):

        db_name = db
        path = "../DB/"+db_name+"/"
        select_pattern1 = r"SELECT \* FROM [\w]+;"
        select_pattern2 = r"SELECT [\w,\s]+ FROM [\w]+;"
        select_pattern3 = r"select \* from [\w]+;"
        select_pattern4 = r"select [\w,\s]+ from [\w]+;"

        select_check1 = re.search(select_pattern1, query)
        select_check2 = re.search(select_pattern2, query)
        select_check3 = re.search(select_pattern3, query)
        select_check4 = re.search(select_pattern4, query)

        parsed_for_letters = query.split(" ")
        if(parsed_for_letters[0] == "SELECT"):
            if(select_check1 == None and select_check2 == None):
                print("Syntax Invalid")
                return
        if(parsed_for_letters[0] == "select"):
            print(parsed_for_letters[0])
            if(select_check3 == None and select_check4 == None):
                print("Syntax Invalid")
                return

        # table name for path
        parsed_for_table_name = query.split(" ")
        table_name = parsed_for_table_name[len(
            parsed_for_table_name)-1]
        table_name = table_name.replace(";", "")
        full_path = path+table_name+".csv"

        if(os.path.exists(full_path) == False):
            print("Table does not exist")
            return

        df = pd.read_csv(full_path, index_col=False)

        if(select_check1 != None or select_check3 != None):
            print(df.to_string(index=False))

        else:
            parsed_query = query.split(" ")
            # print(parsed_query)

            column_names = []
            for i in range(1, len(parsed_query)):
                if(parsed_query[i] == "FROM" or parsed_query[i] == "from"):
                    break

                s1 = parsed_query[i].replace(",", "")
                s1 = s1.strip()
                column_names.append(s1)

            # matching columns of table and query
            for c in column_names:
                if(c not in df.columns):
                    print("Column does not exist")
                    return

            # print(column_names)
            df = df[column_names]
            print(df.to_string(index=False))
            return
    # ----------------------------------------------------------delete----------------------------------------

    def delete(self, query, db, user):
        db_name = db
        delete_pattern = r"DELETE FROM (.*) WHERE (.*) = (.*);"
        updateRegex = re.compile(delete_pattern)
        path = "../DB/"+db_name+"/"

        try:
            if re.match(delete_pattern, query):
                data = updateRegex.search(query)
                table_name = data.groups()[0]
                where_col = data.groups()[1]
                where_val = data.groups()[2]
                if "'" in where_val:
                    where_val = where_val.replace("'", "")

                table_file = path + table_name + ".csv"
                df = pd.read_csv(table_file, index_col=where_col)

                df.drop([where_val], inplace=True)

                df.to_csv(table_file)
        except:
            print("DELETE operation cannot be performed")

    # ---------------------------------------------------------------------------parsing------------------------------
    def parsing(self, query, db, user):
        # ---------------------------------------------------print remove
        print("parsing method called")
        query = query.strip()
        parsed_query = query.split(" ")
        if(parsed_query[0] == "INSERT" or parsed_query[0] == 'insert'):
            self.insert(query, db, user)

        if(parsed_query[0] == "update" or parsed_query[0] == 'UPDATE'):
            self.update(query, db, user)
        if(parsed_query[0] == "SELECT"):
            self.select(query, db, user)
        if(parsed_query[0] == "DELETE"):
            self.delete(query, db, user)

    def update(self, query, db, user):
        dbName = db
        path = "../DB/"+dbName+"/"
        try:
            update_pattern = r'UPDATE ([\w]*) SET (.*) WHERE (.*);'
            update_pattern2 = r'update ([\w]*) set (.*) where (.*);'
            updateRegex = re.compile(update_pattern)

            # table exist or not
            if(re.search(update_pattern, query) or re.search(update_pattern2, query)):
                parsed_for_table_name = query.split(" ")
                table_name = parsed_for_table_name[1]
                full_path = path+table_name+".csv"

                # if table exists or not
                if(os.path.exists(full_path) == False):
                    print("Table does not exist")
                    return

            if re.match(update_pattern, query):
                data = updateRegex.search(query)
                table_name = data.groups()[0]
                update_info = data.groups()[1]
                where_clause = data.groups()[2].split("=")
                where_col = where_clause[0].strip()
                where_val = where_clause[1].strip()
                if "'" in where_val:
                    where_val = where_val.replace("'", "")

                table_file = "../DB/" + dbName + "/" + table_name + ".csv"
                if "," in update_info:
                    update_item = update_info.split(",")
                    for item in update_item:
                        update_clause = item.split("=")
                        update_col = update_clause[0].strip()
                        update_val = update_clause[1].strip()
                        if "'" in update_val:
                            update_val = update_val.replace("'", "")

                        df = pd.read_csv(table_file)
                        df1 = df.loc[df[where_col] == where_val]
                        # print("df1", len(df1.index))
                        df.loc[df[where_col] == where_val,
                               [update_col]] = update_val
                        df.to_csv(table_file, index=False)
                    print("Table updated")
        except:
            print("update operation cannot be performed")

    def update(self, query):
        dbName = "DB1"
        try:
            update_pattern = r'UPDATE ([\w]*) SET (.*) WHERE (.*);'
            updateRegex = re.compile(update_pattern)
            if re.match(update_pattern, query):
                data = updateRegex.search(query)
                table_name = data.groups()[0]
                update_info = data.groups()[1]
                where_clause = data.groups()[2].split("=")
                where_col = where_clause[0].strip()
                where_val = where_clause[1].strip()
                if "'" in where_val:
                    where_val = where_val.replace("'", "")

                table_file = "../DB/" + dbName + "/" + table_name + ".csv"
                if "," in update_info:
                    update_item = update_info.split(",")
                    for item in update_item:
                        update_clause = item.split("=")
                        update_col = update_clause[0].strip()
                        update_val = update_clause[1].strip()
                        if "'" in update_val:
                            update_val = update_val.replace("'", "")

                        df = pd.read_csv(table_file)
                        df1 = df.loc[df[where_col] == where_val]
                        # print("df1", len(df1.index))
                        df.loc[df[where_col] == where_val, [update_col]] = update_val
                        df.to_csv(table_file, index=False)

                # INSERT INTO Customers (CustomerName, ContactName, Address, City, PostalCode, Country) VALUES ('Cardinal', 'Tom B. Erichsen', 'Skagen 21', 'Stavanger', '4006', 'Norway');
                # UPDATE table1 SET City = 'Frankfurt', Address = 'Quinpool towers' WHERE CustomerName = 'Mihir';
        except:
            print("update operation cannot be performed")

def main():

    print("python main function")
    choice = int(
        input("Enter your choice: \n 1. Login \n 2. Query \n 3. Logout \n"))

    def login():
        print(" Login function called")

    def query():
        print("query called")
        query = input("Enter Query: ")
        p = parsor()
        p.parsing(query, "DB1", "")

    def logout():
        print("Logout function called")

# If user enters invalid option then this method will be called
    def default():
        print("Incorrect choice")

# Dictionary Mapping
    switcher = {
        1: login,
        2: query,
        3: logout,

    }

    switcher.get(choice, default)()


if __name__ == '__main__':
    main()
