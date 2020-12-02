import os
import csv
import re

from src.start import *
from src.log_manager import write_log

def useDbQuery(user, query) -> str:
    dbList = [ item for item in os.listdir("../DB") if os.path.isdir(os.path.join("../DB", item)) ]
    words = query.split(" ")
    if len(words) == 2 and query.endswith(";"):
        dbname = words[1][:-1]
        print("dbname: ", dbname)
        if dbname in dbList:
            dir = "../DB/" + dbname
            with open(dir + "/dbuser.csv", "r+") as userfile:
                userList = userfile.readlines()
            userList = [x.strip() for x in userList]
            userfile.close()
            if user.username in userList:
                print("access granted for user", user.username, " to database ", dbname)

                write_log("access granted for user" + user.username + " to database " + dbname)
                return dbname
            else:
                print("access not granted for user", user.username, " to database ", dbname)
                return dbname
        else:
            print(dbname, " database does not exists. Please create one.")
            return ""

    else:
        print("Incorrect syntax for USE DB query")
        return ""


def addUserToDbQuery(user, query):
    try:
        dbList = [item for item in os.listdir("../DB") if os.path.isdir(os.path.join("../DB", item))]
        regex = r'ADD\sUSER\s(.*)\sTO\s(.*);'
        addUserRegex = re.compile(regex)
        if re.match(regex, query):
            print("query valid")
            data = addUserRegex.search(query)
            username = data.groups()[0]
            db = data.groups()[1]
            if db in dbList:
                dir = "../DB/" + db
                with open(dir + "/dbuser.csv", "a+") as userfile:
                    userfile.seek(0)
                    userList = userfile.readlines()

                    userList = [x.strip() for x in userList]
                    print("userlist in add user to db", userList)
                    if len(userList) >= 2:
                        print("Already two users have access to database ", db)

                    else:
                        if username in userList:
                            print(username, "already have access to database", db)
                        else:
                            userfile.write(username + "\n")
                            print("User", username + " given access to database ", db)
                            write_log("User " + username + " given access to database " + db)
            else:
                print("Database ", db, " does not exists")
        else:
            print("invalid query, ", query)
    except:
        print("USER CANNOT BE ADDED")


def createDbQuery(user, query):
    try:

        dbList = [item for item in os.listdir("../DB") if os.path.isdir(os.path.join("../DB", item))]
        regex = r'CREATE\sDATABASE\s(.*);'
        createDbRegex = re.compile(regex)
        if re.match(regex, query):
            print("query valid")
            data = createDbRegex.search(query)
            dbname = data.groups()[0]
            if dbname in dbList:
                print("database", dbname, "already exists")
            else:
                parentDir = "../DB"
                path = os.path.join(parentDir, dbname)
                os.mkdir(path)
        else:
            print("invalid query, ", query)
    except:
        print("DATABASE", dbname, "cannot be created")