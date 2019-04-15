library(RSQLite)

## connect to db
con <- dbConnect(drv=RSQLite::SQLite(), dbname="Northwind.sqlite")

## list all tables
tables <- dbListTables(con)

## exclude sqlite_sequence (contains table information)
tables <- tables[tables != "sqlite_sequence"]


for(i in seq_along(tables)){
   assign(paste("dataframe", tables[i],sep ="_"),dbGetQuery(con, paste('select * from', tables[i])))
}
    


