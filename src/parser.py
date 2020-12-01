import csv
import re
import pandas as pd


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

    def parsing(self, query):
        # ---------------------------------------------------print remove
        print("parsing method called")
        query = query.strip()
        parsed_query = query.split(" ")
        if(parsed_query[0] == "INSERT" or parsed_query[0] == 'insert'):
            self.insert(query)

        if(parsed_query[0] == "update" or parsed_query[0] == 'UPDATE'):
            print("update method called")


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
