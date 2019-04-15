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

#copy employees into employees_dimension and remove missing values
employees_dimension<-Employees
for(i in 1:nrow(employees_dimension)){
  for(j in 3:ncol(employees_dimension)){
    if(is.na(employees_dimension[i,j])){
      employees_dimension[i,j]<-"Missing"
    }
  }
}
#sort by CompanyName
employees_dimension<-employees_dimension[order(employees_dimension$LastName),]
#create primary key for employees_dimension
employees_dimension$employee_key<-1:nrow(employees_dimension)

#suppliers_dimension
#copy Suppliers into suppliers_dimension and remove missing values
suppliers_dimension<- Suppliers
for(i in 1:nrow(suppliers_dimension)){
  for(j in 3:ncol(suppliers_dimension)){
    if(is.na(suppliers_dimension[i,j])){
      suppliers_dimension[i,j]<-"Missing"
    }
  }
}

#sort by CompanyName
suppliers_dimension <- suppliers_dimension[order(suppliers_dimension$CompanyName),]
#create primary key for customers_dimension
suppliers_dimension$supplier_key<-1:nrow(suppliers_dimension)

