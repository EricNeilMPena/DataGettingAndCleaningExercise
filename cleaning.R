library(RSQLite)
library(DBI)
library(dplyr)
#extract data from Northwind DB
con1 <- dbConnect(RSQLite::SQLite(), dbname="Northwind.sqlite")
alltables <- dbListTables(con1)
tables<-as.data.frame(alltables)
#extract tables from northwind db
for (i in 1:length(alltables)) {
      if(!i==9){
            assign(paste0(tables[i,]),dbGetQuery(con1,paste0('select * from ',tables[i,]) ))
            }
}
#extract tables from datawarehouse
con2 <- dbConnect(RSQLite::SQLite(), dbname="DataWarehouse.sqlite")
alltables2<- dbListTables(con2)
tables2<-as.data.frame(dbListTables(con2))
for (i in 1:length(alltables2)) {
      if(!i==9){
            assign(paste0(tables2[i,]),dbGetQuery(con2,paste0('select * from ',tables2[i,]) ))
      }
}
#copy customers into customers_dimension and remove missing values
customers_dimension<-Customers
for(i in 1:nrow(customers_dimension)){
      for(j in 3:ncol(customers_dimension)){
            if(is.na(customers_dimension[i,j])){
                  customers_dimension[i,j]<-"Missing"
            }
      }
}
#sort by CompanyName
customers_dimension<-customers_dimension[order(customers_dimension$CompanyName),]
#create primary key for customers_dimension
customers_dimension$customer_key<-1:nrow(customers_dimension)

