# Connect to mysql server
## Library
library(RMySQL)

## Settings freemysqlhosting.net (max 5MB)
db_name_fh <- "sql12637820"
db_user_fh <- "sql12637820"
db_host_fh <- "sql12.freemysqlhosting.net"
db_pwd_fh <- "ptskPrlkkw"
db_port_fh <- 3306

## Connect to remote server database
mydb.fh <-  dbConnect(RMySQL::MySQL(), user = db_user_fh, password = db_pwd_fh,
                      dbname = db_name_fh, host = db_host_fh, port =db_port_fh)
mysql_conn <- mydb.fh

# Connect to sqlite database
library(RSQLite)
fpath = "."
dbfile = "Sales.db"
sqlite_conn <- dbConnect(RSQLite::SQLite(), file.path(fpath, dbfile))

# Drop tables if existed
dbExecute(mysql_conn,"DROP TABLE IF EXISTS salestxn;")
dbExecute(mysql_conn,"DROP TABLE IF EXISTS products;")
dbExecute(mysql_conn,"DROP TABLE IF EXISTS reps;")
dbExecute(mysql_conn,"DROP TABLE IF EXISTS customers;")
dbExecute(mysql_conn, "DROP TABLE IF EXISTS dimDate;")
dbExecute(mysql_conn, "DROP TABLE IF EXISTS product_facts;")
dbExecute(mysql_conn, "DROP TABLE IF EXISTS rep_facts;")

## Create products table
sql <- "CREATE TABLE IF NOT EXISTS products(
          pid INTEGER NOT NULL,
          prodname TEXT NOT NULL,
          PRIMARY KEY (pid)
        );"
dbExecute(mysql_conn,sql)

## Create reps table
sql <- "CREATE TABLE IF NOT EXISTS reps(
          repid INTEGER NOT NULL,
          firstname TEXT NOT NULL,
          lastname TEXT NOT NULL,
          territory TEXT NOT NULL,
          PRIMARY KEY (repid)
        );"
dbExecute(mysql_conn,sql)

## Create customers table
sql <- "CREATE TABLE IF NOT EXISTS customers(
          custid INTEGER NOT NULL,
          custname TEXT NOT NULL,
          country TEXT NOT NULL,
          PRIMARY KEY (custid)
        );"
dbExecute(mysql_conn,sql)

## Create salestxn table
sql <- "CREATE TABLE IF NOT EXISTS salestxn(
          txnid INTEGER NOT NULL,
          date DATE NOT NULL,
          qty INTEGER NOT NULL,
          amount INTEGER NOT NULL,
          repid INTEGER NOT NULL,
          custid INTEGER NOT NULL,
          pid INTEGER NOT NULL,
          PRIMARY KEY (txnid),
          FOREIGN KEY (repid) REFERENCES reps (repid),
          FOREIGN KEY (custid) REFERENCES customers (custid),
          FOREIGN KEY (pid) REFERENCES products (pid)
        );"
dbExecute(mysql_conn,sql)

# Fetch the salestxn data from sqlite database and write into mysql server
query <- paste("SELECT * FROM", "salestxn")
result <- dbSendQuery(sqlite_conn, query)
data <- dbFetch(result)

dbWriteTable(mysql_conn, "salestxn", data, overwrite = TRUE)

dbClearResult(result)

# Fetch the reps data from sqlite database and write into mysql server
query <- paste("SELECT * FROM", "reps")
result <- dbSendQuery(sqlite_conn, query)
data <- dbFetch(result)

dbWriteTable(mysql_conn, "reps", data, overwrite = TRUE)

dbClearResult(result)

# Fetch the customers data from sqlite database and write into mysql server
query <- paste("SELECT * FROM", "customers")
result <- dbSendQuery(sqlite_conn, query)
data <- dbFetch(result)

dbWriteTable(mysql_conn, "customers", data, overwrite = TRUE)

dbClearResult(result)

# Fetch the products data from sqlite database and write into mysql server
query <- paste("SELECT * FROM", "products")
result <- dbSendQuery(sqlite_conn, query)
data <- dbFetch(result)

dbWriteTable(mysql_conn, "products", data, overwrite = TRUE)

dbClearResult(result)

# Create a dimensional table called dimDate
sql <- "CREATE TABLE dimDate (
          did INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
          tdate DATE NOT NULL,
          month INT NOT NULL,
          quarter INT NOT NULL,
          year INT NOT NULL
        );"
dbExecute(mysql_conn, sql)

# Insert data into dimDate table
sql <- "INSERT INTO dimDate (tdate,month,quarter,year) 
         SELECT DISTINCT s.date, month(s.date) AS month, quarter(s.date) AS quarter, year(s.date) AS year
         FROM salestxn s"
         
dbExecute(mysql_conn, sql)

# Create a fact table called product_facts
sql <- "CREATE TABLE IF NOT EXISTS product_facts(
          prodfactid INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
          productName TEXT NOT NULL,
          sold INT NOT NULL,
          region TEXT NOT NULL,
          dateid INT NOT NULL
        );"
dbExecute(mysql_conn, sql)

# Insert data into product_facts table
sql <- "INSERT INTO product_facts(productName,sold,region,dateid)
         SELECT p.prodname AS productName, s.amount AS sold, c.country as region, d.did
         FROM salestxn s 
         JOIN products p ON s.pid = p.pid
         JOIN customers c ON s.custid = c.custid
         JOIN dimDate d ON s.date = d.tdate;"
dbExecute(mysql_conn, sql)

# Create a fact table called rep_facts
sql <- "CREATE TABLE IF NOT EXISTS rep_facts(
          repfactid INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
          repfirstname TEXT NOT NULL,
          replastname TEXT NOT NULL,
          prodname TEXT NOT NULL,
          sold INT NOT NULL,
          dateid INT NOT NULL
        );"
dbExecute(mysql_conn, sql)

# Insert data into rep_facts table
sql <- "INSERT INTO rep_facts(repfirstname,replastname,prodname,sold,dateid)
         SELECT r.firstname,r.lastname,p.prodname AS productName, s.amount AS sold, d.did
         FROM salestxn s 
         JOIN products p ON s.pid = p.pid
         JOIN reps r ON s.repid = r.repid
         JOIN dimDate d ON s.date = d.tdate;"
dbExecute(mysql_conn, sql)


# Test analytical queries
## 1.What is the total sold for each quarter of 2020 for all products?
dbGetQuery(mysql_conn, "SELECT SUM(p.sold) AS totalsold, d.quarter, d.year 
                        FROM product_facts p
                        JOIN dimDate d ON p.dateid = d.did
                        WHERE d.year = 2020
                        GROUP BY d.quarter")

## 2.What is the total sold for each quarter of 2020 for 'Alaraphosol'?
dbGetQuery(mysql_conn, "SELECT p.productNAme, SUM(p.sold) AS totalsold, d.quarter, d.year 
                        FROM product_facts p
                        JOIN dimDate d ON p.dateid = d.did
                        WHERE d.year = 2020 AND p.productName = 'Alaraphosol'
                        GROUP BY d.quarter")

## 3.Which product sold the best in 2020?
dbGetQuery(mysql_conn, "SELECT p.productName, SUM(p.sold) AS totalsold 
                        FROM product_facts p
                        JOIN dimDate d ON p.dateid = d.did
                        WHERE d.year = 2020
                        GROUP BY p.productName
                        ORDER BY totalsold DESC
                        LIMIT 1
                        ")

## 4.How much did each sales rep sell in 2020?
dbGetQuery(mysql_conn, "SELECT r.repfirstname, r.replastname, SUM(r.sold) AS totalsold 
                        FROM rep_facts r
                        JOIN dimDate d ON r.dateid = d.did
                        WHERE d.year = 2020
                        GROUP BY r.repfirstname, r.replastname
                        ")



# Disconnect from database
dbDisconnect(mysql_conn)
dbDisconnect(sqlite_conn)