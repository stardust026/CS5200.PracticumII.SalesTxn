# Load the library
library(RSQLite)
library(XML)

# Create the file path and database name
fpath = "."
dbfile = "Sales.db"

# Connect to database
dbcon <- dbConnect(RSQLite::SQLite(), file.path(fpath, dbfile))

# Question 1&2: Create and realize a normalized relational schema in SQLite
## Drop tables if they are existed
dbExecute(dbcon,"DROP TABLE IF EXISTS salestxn;")
dbExecute(dbcon,"DROP TABLE IF EXISTS products;")
dbExecute(dbcon,"DROP TABLE IF EXISTS reps;")
dbExecute(dbcon,"DROP TABLE IF EXISTS customers;")

## Create products table
sql <- "CREATE TABLE IF NOT EXISTS products(
          pid INTEGER NOT NULL,
          prodname TEXT NOT NULL,
          PRIMARY KEY (pid)
        );"
dbExecute(dbcon,sql)

## Create reps table
sql <- "CREATE TABLE IF NOT EXISTS reps(
          repid INTEGER NOT NULL,
          firstname TEXT NOT NULL,
          lastname TEXT NOT NULL,
          territory TEXT NOT NULL,
          PRIMARY KEY (repid)
        );"
dbExecute(dbcon,sql)

## Create customers table
sql <- "CREATE TABLE IF NOT EXISTS customers(
          custid INTEGER NOT NULL,
          custname TEXT NOT NULL,
          country TEXT NOT NULL,
          PRIMARY KEY (custid)
        );"
dbExecute(dbcon,sql)

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
dbExecute(dbcon,sql)

# Question 3&4: Load all XML files from txn-xml folder into R

## Define folder path
folder_path <- ".//txn-xml"

## List reps file name
reps_file <- "pharmaReps.xml"

## List all the salesTxn files name
salesTxn_files <- list.files(path = folder_path, pattern = "pharmaSalesTxn.*\\.xml")

## Create reps data frame
reps_df <- data.frame(repid = integer(),
                      firstname = character(),
                      lastname = character(),
                      territory = character(),
                      stringsAsFactors = F)

## Create customers data frame
cust_df <- data.frame(custid = integer(),
                      custname = character(),
                      country = character(),
                      stringsAsFactors = F)

## Create products data frame
prod_df <- data.frame(pid = integer(),
                      prodname = character(),
                      stringsAsFactors = F)

## Create salesTxn data frame
salesTxn_df <- data.frame(txnid = integer(),
                          date = character(),
                          qty = integer(),
                          amount = integer(),
                          repid = integer(),
                          custid = integer(),
                          pid = integer())


## Create customerExists function
customerExists <- function (custName, custCountry, custDF)
{
  ### check if that row is already in the data frame
  n <- nrow(custDF)
  if(n == 0){
    ### data frame is empty, so can't exist
    return(0)
  }
  for(a in 1:n){
    ### check if all columns match for a row; 
    if (custDF[a,2] == custName && custDF[a,3] == custCountry){
      ### found a match; return it's ID
      return(a)
    }
  }
  return(0)
}

## Create productExists function
productExists <- function (productName, prodDF)
{
  ### check if that row is already in the data frame
  n <- nrow(prodDF)
  if(n == 0){
    ### data frame is empty, so can't exist
    return(0)
  }
  for(a in 1:n){
    ### check if all columns match for a row; 
    if (prodDF[a,2] == productName){
      ### found a match; return it's ID
      return(a)
    }
  }
  return(0)
}

## Load reps XML file into data frame
reps_data <- xmlParse(paste0(folder_path,"/",reps_file))
reps_r <- xmlRoot(reps_data)
numreps <- xmlSize(reps_r)
for(i in 1:numreps){
  # Get the next sale rep
  aRep <- reps_r[[i]]
  
  # Get the sale rep attribute
  a <- xmlAttrs(aRep)
  
  # Get the rep id
  repID <- as.integer(gsub("\\D+","",a[1]))
  
  # add repid into reps data frame
  reps_df[i,1] <- repID
  
  # add other elements into reps data frame
  reps_df[i,2] <- xpathSApply(aRep, ".//firstName", xmlValue)
  reps_df[i,3] <- xpathSApply(aRep, ".//lastName", xmlValue)
  reps_df[i,4] <- xpathSApply(aRep, ".//territory", xmlValue)
}

## Load all salesTxn XML files into several data frames
for(file in salesTxn_files){
  salesTxn_data <- xmlParse(paste0(folder_path,"/",file))
  salesTxn_r<- xmlRoot(salesTxn_data)
  numreps <- xmlSize(salesTxn_r)
  for(i in 1:numreps){
    # Get the next sale transaction
    aTxn <- salesTxn_r[[i]]
    
    # Get the txnID and add it to data frame
    txnID <- nrow(salesTxn_df)+1
    salesTxn_df[txnID,1] <- txnID[[1]]
    
    # Get date and add it to data frame
    txnDate <- xpathApply(aTxn, ".//date", xmlValue)
    txnDate <- as.Date(txnDate[[1]],format = "%m/%d/%Y")
    salesTxn_df[txnID,2] <- as.character(txnDate)
    
    # Get qty and add it to data frame
    salesTxn_df[txnID,3] <- xpathApply(aTxn, ".//qty", xmlValue)
    
    # Get amount and add it to data frame
    salesTxn_df[txnID,4] <- xpathApply(aTxn, ".//amount", xmlValue)
    
    # Get repID and add it to data frame
    salesTxn_df[txnID,5] <- xpathApply(aTxn, ".//repID", xmlValue)
    
    # Add customer information to customer data frame
    # and create new customer ID if it is not existed
    custname <- xpathApply(aTxn, ".//cust", xmlValue)
    country <- xpathApply(aTxn, ".//country", xmlValue)
    custID <- customerExists(custname, country, cust_df)
    if(custID == 0){
      custID <- nrow(cust_df)+1
      cust_df[custID, 2] <- custname
      cust_df[custID, 3] <- country
      cust_df[custID, 1] <- custID
    }
    salesTxn_df[txnID,6] <- custID
    
    # Add product information to customer data frame
    # and create new product ID if it is not existed
    prodname <- xpathApply(aTxn, ".//prod", xmlValue)
    prodID <- productExists(prodname, prod_df)
    if(prodID == 0){
      prodID <- nrow(prod_df)+1
      prod_df[prodID, 2] <- prodname
      prod_df[prodID, 1] <- prodID
    }
    salesTxn_df[txnID,7] <- prodID
  }
}


## Write data frames into table
dbWriteTable(dbcon, "reps", reps_df, append = T)
dbWriteTable(dbcon, "products", prod_df, append = T)
dbWriteTable(dbcon, "customers", cust_df, append = T)
dbWriteTable(dbcon, "salestxn", salesTxn_df, append = T)


# Disconnect from database
dbDisconnect(dbcon)