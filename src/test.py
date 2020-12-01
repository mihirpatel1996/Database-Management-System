import re

# update_re = r'UPDATE (.*) SET (.*) WHERE (.*);'
# update_query = "UPDATE Customers SET ContactName = 'Alfred Schmidt', City= 'Frankfurt' WHERE CustomerID = 1;"
#
# updateQueryRegex = re.compile(update_re)
# data = updateQueryRegex.search(update_query)
# print("data.groups()", data.groups())
def selectQuery(query):
    if "WHERE" in query:
        select_re = r'SELECT (.*) FROM (.*) WHERE (.*)?;'
    else:
        select_re = r'SELECT (.*) FROM (.*);'
    select_query = "SELECT * FROM Customers WHERE CustomerID=1 AND CustomerID=2;"
    select_query1 = "SELECT * FROM Customers;"
    # select_re = r'SELECT (.*) FROM (.*)(?: WHERE (.*))?;'

    selectQueryRegex = re.compile(select_re)
    # data = selectQueryRegex.search(query)
    # print("data.groups()", data.groups())

    if re.match(select_re, query):
        print("QUERY:", query, "MATCH")
        data = selectQueryRegex.search(query)
        print("data.groups()", data.groups())

selectQuery("SELECT * FROM Customers WHERE CustomerID=1 AND CustomerID=2;")
selectQuery("SELECT * FROM Customers;")

# delete_re = r"DELETE FROM (.*) WHERE (.*)=(.*);"
# delete_query = "DELETE FROM Customers WHERE CustomerName='Alfreds Futterkiste';"
# # delete_query = "DELETE FROM Customers WHERE CustomerName=1;"
# deleteQueryRegex = re.compile(delete_re)
# data = deleteQueryRegex.search(delete_query)
# print("data.groups()", data.groups())