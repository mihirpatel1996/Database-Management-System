import csv
import re
import pandas as pd
import os.path
from os import path


class parsor:

    # INSERT INTO Customers (CustomerName, ContactName, Address, City, PostalCode, Country) VALUES ('Cardinal', 'Tom B. Erichsen', 'Skagen 21', 'Stavanger', '4006', 'Norway');
    def insert(self, query):

        # ------------------------------------print remove
        print("insert called")

        # table pattern does not match special charcters in VALUES
        # table_name_pattern = r"^INSERT\sINTO\s[\w]+\s\([\w\s,]+\)\sVALUES\s\([\w\s,']+\);$"
        table_name_pattern1 = r"^INSERT\sINTO\s[\w]+\s\([\w\s,]+\)\sVALUES\s\([\w\W]+\);$"
        table_name_pattern2 = r"^INSERT\sINTO\s[\w]+\s\([\w,]+\)\sVALUES\s\([\w\W]+\);$"
        table_name_pattern3 = r"^insert\sinto\s[\w]+\s\([\w\s,]+\)\svalues\s\([\w\W]+\);$"
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

        parsed_query = query.split(' ')
        print("parsed_query:", parsed_query)

        parsed_query1 = query.split(",")
        print("parsed_query1:", parsed_query1)

        # syntax for insert length checked
        if(len(parsed_query) <= 6):
            print("Syntax Invalid")
            return

        # to find values of column name
        parentheses_count = 0
        column_name_flag = False
        column_names = []

        for i in range(0, len(parsed_query1)):
            print("parsed_query value:", parsed_query1[i])

            if(')' in parsed_query1[i]):
                print(") matched:", parsed_query1[i])
                parsed_array = parsed_query1[i].split(" ")
                print("parsed_array:", parsed_array)
                for i in range(0, len(parsed_array)):
                    if(')' in parsed_array[i]):
                        replaced_string = parsed_array[i].replace(")", "")
                        column_names.append(replaced_string)
                        column_name_flag = False
                break

            if('(' in parsed_query1[i]):
                column_name_flag = True
                print("( matched:", parsed_query1[i])
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

        """for i in range(3, len(parsed_query)):
            if(parsed_query[i] == "VALUES" or parsed_query[i] == "values"):
                break

            if('(' in parsed_query[i]):
                column_name_flag = True
                replaced_string = parsed_query[i].replace("(", "")
                replaced_string = replaced_string.replace(",", "")
                # replaced_string = replaced_string.strip()
                column_names.append(replaced_string)
                continue

            if(')' in parsed_query[i]):
                replaced_string = parsed_query[i].replace(")", "")
                replaced_string = replaced_string.replace(",", "")
                # replaced_string = replaced_string.strip()
                column_names.append(replaced_string)
                column_name_flag = False

            if(column_name_flag == True):
                replaced_string = parsed_query[i].replace(",", "")
                # replaced_string = replaced_string.strip()
                column_names.append(replaced_string)"""

        # --------------------------------------print remove
        print("column_names: ", column_names)

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

        # --------------------------------------print remove
        print("Values: ", values)

        # to check if column names and values match
        if(len(column_names) != len(values)):
            print("Invalid Syntax")
            return

        d = {column_names[i]: values[i]
             for i in range(0, len(column_names), 1)}

        # ------------------------------------------------remove dictionary
        print(d)
        # field_names = ['id', 'name', 'age']
        # d = {'id': 1, 'name': "Mihir", 'age': 24}
        # print(d)

        # writing dictionary in file
        with open(r'../DB/DB1/table1.csv', 'a', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=column_names)
            writer.writeheader()
            writer.writerow(d)
            print("Row written to file")

        return

    # select query function
    def select(self, query):
        print("print called")
        path = "../DB/DB1/"
        select_pattern1 = r"SELECT \* FROM [\w]+;"
        select_pattern2 = r"SELECT [\w,\s]+ FROM [\w]+;"

        select_check1 = re.search(select_pattern1, query)
        select_check2 = re.search(select_pattern2, query)

        if(select_check1 == None and select_check2 == None):
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

        if(select_check1 != None):
            print(df.to_string(index=False))
        else:
            parsed_query = query.split(" ")
            print(parsed_query)

            column_names = []
            for i in range(1, len(parsed_query)):
                if(parsed_query[i] == "FROM"):
                    break
                s1 = parsed_query[i].replace(",", "")
                s1 = s1.strip()
                column_names.append(s1)

            print(column_names)
            df = df[column_names]
            print(df.to_string(index=False))

    def parsing(self, query):
        # ---------------------------------------------------print remove
        print("parsing method called")
        query = query.strip()
        parsed_query = query.split(" ")
        if(parsed_query[0] == "INSERT" or parsed_query[0] == 'insert'):
            self.insert(query)

        if(parsed_query[0] == "update" or parsed_query[0] == 'UPDATE'):
            self.update(query)
        if(parsed_query[0] == "SELECT"):
            self.select(query)

    def update(self, query):
        print("update called")
        update_pattern1 = r'UPDATE ([\w]*) SET (.*) WHERE (.*);'
        # update_pattern2 = r'update (.*) set (.*) where (.*);'
        # UPDATE Customers SET ContactName = 'Alfred Schmidt', City = 'Frankfurt' WHERE CustomerID = 1;
        df = pd.read_csv("../DB/DB1/table1.csv")
        print(df)

        update_query_check1 = re.search(update_pattern1, query)
        # update_query_check2 = re.search(update_pattern2, query)
        if(update_query_check1 == None):
            print("Syntax Invalid")
            return
        # parsing of query for checking table exists or not

        # parsing of query to check column exists or not
        parsed_query_for_key = query.split('WHERE')

        print("WhERE split array:", parsed_query_for_key)
        # last value in array is where column cluase
        where_clause = parsed_query_for_key[len(parsed_query_for_key)-1]
        print("Id :", where_clause)
        id_array = where_clause.split('=')
        id_column = id_array[0].strip()
        id_value = id_array[1].strip()
        id_value = id_value.replace("'", "")
        id_value = id_value.replace(";", "")
        print("id column:", id_column+" id_value:", id_value)

        # getting colums and values to replace
        parsed_for_columns = query.split("'")
        print("parsed by ' array:", parsed_for_columns)

        column_value_array = []
        for i in range(0, len(parsed_for_columns)):
            if("WHERE" in parsed_for_columns[i]):
                break
            if("SET" in parsed_for_columns[i]):
                print("SET matched:", parsed_for_columns[i])
                space_split = parsed_for_columns[i].split("SET")
                print("space split:", space_split)
                s1 = space_split[len(space_split)-1]
                s1 = s1.replace("=", "")
                s1 = s1.strip()
                column_value_array.append(s1)
                continue
            else:
                # print("parsed_for_columns[%d]:%s" % (i, parsed_for_columns[i]))
                s1 = parsed_for_columns[i].replace("'", "")
                s1 = s1.replace(",", "")
                s1 = s1.replace("=", "")
                s1 = s1.strip()
                print("col_val:", s1)
                column_value_array.append(s1)

        print("columns:", column_value_array)
        # table columns
        for col in df.columns:
            print(col)

        # df.set_index('CustomerName', inplace=True)
        '''for i in range(0, len(df.columns)):
            df.set_index(df.columns[0])
            print(df.columns[i])'''

        # checking id colummn exists or not in table
        if(id_column in df.columns):
            print("id column found")
        else:
            print("id column not found")
        print("type of id:", type(id_column))
        print("type of name:", type(id_value))

        column_names = []
        column_values = []
        # getting column_names and values
        for i in range(0, len(column_value_array)):
            if(i % 2 == 0):
                column_names.append(column_value_array[i])
            else:
                column_values.append(column_value_array[i])

        print("column names:", column_names)
        print("column values:", column_values)
        # checking other column exist in table or not
        # matched_columns = df.loc[[df[id_column] ==
        #                          id_value], "City", "Address"] = "Frankfurk", "6967 Bayers road"
        # df.at[id_value, column_names] = column_values
        df = df.loc[df[id_column] == id_value]
        # df.replace(to_replace="Address", value="quinpool towers")
        # df.loc['Mihir', 'Address'] = 'quinpool towers'

        df.replace(to_replace=['Mihir', ],
                   value=['Michael'],
                   inplace=True)
        print(df)
        # df.to_csv('../DB/DB1/table1.csv', mode='a', header=False, index=False)
        print("Added")
        # print("matched columns:\n", matched_columns)
        # print(matched_records)
        # INSERT INTO Customers (CustomerName, ContactName, Address, City, PostalCode, Country) VALUES ('Cardinal', 'Tom B. Erichsen', 'Skagen 21', 'Stavanger', '4006', 'Norway');
        # UPDATE table1 SET City = 'Frankfurt', Address = 'Quinpool towers' WHERE CustomerName = 'Mihir';


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
        p.parsing(query)

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
