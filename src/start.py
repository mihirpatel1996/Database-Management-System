import sys
import csv
import pandas as pd
import os
import re

from src.log_manager import write_log
from src.dbOperations import *
# from src.createTable import *
from src.tableOperations import *
from src.sqlDumpOperation import *
from src.parser import *
from src.erdOperation import *


class User(object):
    username = ""

    def doLogin(self):
        username = input("Enter Username: ")
        password = input("Enter Password: ")
        with open('../users.csv', 'r', newline='') as file:
            existingUsers = [line for line in csv.reader(file, delimiter=',')]
            curr_user = [username, password]
            if curr_user in existingUsers:
                print("login successful")
                write_log(username + " logged in")
                self.username = username
            else:
                print("login not successful, try again!")
                initPrompt(user)

    def doSignUp(self):
        username = input("Enter Username: ")
        password = input("Enter Password: ")
        df = pd.read_csv("../users.csv")
        existing_users = df["user"].tolist()
        with open('../users.csv', 'a+') as file:
            if username not in existing_users:
                writer = csv.writer(file)
                writer.writerow([username, password])
                write_log(username + " signed up")
            else:
                print("User already exists")
                initPrompt(user)

    def doSignOut(self):
        print("Signed Out Successfully")
        write_log(self.username + " signed out")
        self.username = ""


def initPrompt(user) -> User:
    if user is None:
        user = User()
    print("Select a option please")
    while True:
        print("1. Login")
        print("2. Sign Up")
        print("3. Exit")
        option = input()
        if option == "1":
            user.doLogin()
            print("user:", user.username)
            user = option_menu(user, "")
            # break
        elif option == "2":
            user.doSignUp()
            initPrompt(user)
        elif option == "3":
            # perform cleanup tasks here
            sys.exit()
        else:
            print("Please select a correct option")
            initPrompt(user)
        return user


def option_menu(user, dbName) -> User:
    while True:
        print("Select a option please")
        print("1. Write Query")
        print("2. Sign Out")
        option = input()
        if option == "1":
            query = input("Enter Query: ")
            identifyQuery(user, query, dbName)
            break
        elif option == "2":
            user.doSignOut()
            user = initPrompt(user)
            break
        else:
            print("Please slect correct option")
            option_menu(user, dbName)
        return user

def identifyQuery(user, query, dbName):
    # print("username in identifyQuery", user.username)
    parser = parsor()
    crud_keywords = ["INSERT", "insert", "SELECT", "select", "UPDATE", "update", "DELETE", "delete"]
    words = query.split(" ")
    if words[0] == "USE":
        db = useDbQuery(user, query)
        option_menu(user, db)
    elif words[0] == "ADD" and words[1] == "USER":
        addUserToDbQuery(user, query)
        option_menu(user, dbName)
    elif words[0] == "CREATE" and words[1] == "DATABASE":
        createDbQuery(user, query)
        option_menu(user, dbName)
    elif words[0] == "CREATE" and words[1] == "TABLE":
        if dbName == "":
            print("please run USE DB command first")
        else:
            createTableQuery(dbName, query, user)
        option_menu(user, dbName)
    elif words[0] == "DROP" and words[1] == "TABLE":
        if dbName == "":
            print("please run USE DB command first")
        else:
            deleteTableQuery(dbName, query, user)
        option_menu(user, dbName)
    elif words[0] == "DROP" and words[1] == "DB":
        if dbName == "":
            print("please run USE DB command first")
        else:
            dropDbQuery(query, user)
        option_menu(user, dbName)
    elif words[0] == "SQLDUMP":
        if dbName == "":
            print("please run USE DB command first")
        else:
            sqlDumpQuery(query, user)
        option_menu(user, dbName)
    elif words[0] in crud_keywords:
        if dbName == "":
            print("please run USE DB command first")
        else:
            parser.parsing(query, dbName, user)
        option_menu(user, dbName)
    elif words[0] == "CREATE" and words[1] == "ERD":
        if dbName == "":
            print("please run USE DB command first")
        else:
            erdQuery(query,user)
        option_menu(user, dbName)
    else:
        print("QUERY:", query, "not in correct format. Please check!")
        option_menu(user, dbName)

if __name__ == "__main__":
    user = User()
    user = initPrompt(user)
    # user = option_menu(user)