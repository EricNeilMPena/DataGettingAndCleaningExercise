library(RSQLite)
library(DBI)
library(dplyr)
library(lubridate)
detach("package:data.table",unload=TRUE)

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

#4.2
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
customers_dimension<-customers_dimension[,c(12,1:11)]

#4.3
#copy employees into employees_dimension and remove missing values
employees_dimension<-Employees
employees_dimension$Photo<-0
for(i in 1:nrow(employees_dimension)){
      for(j in 3:ncol(employees_dimension)){
            if(is.na(employees_dimension[i,j])){
                  employees_dimension[i,j]<- c("Missing")
            }
      }
}
#sort by LastName
employees_dimension<-employees_dimension[order(employees_dimension$LastName),]
#create primary key for employees_dimension
employees_dimension$employee_key<-1:nrow(employees_dimension)
employees_dimension<-employees_dimension[,c(19,1:18)]

#4.4
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
suppliers_dimension<-suppliers_dimension[,c(13,1:12)]

#4.1
temp<-as.data.frame(1:3652)
order_date_dimension<-rbind(order_date_dimension,temp)
colnames(order_date_dimension)<-"order_date_key"
order_date_dimension$order_date<-seq(ymd("2010-01-01"),ymd("2019-12-31"),by= "1 day")
order_date_dimension$order_day_of_week<-weekdays(order_date_dimension$order_date)
#check type of day
for (i in 1:nrow(order_date_dimension)){
      ifelse(order_date_dimension[i,3]=="Sunday",order_date_dimension$type_of_day[i]<-"Weekend",
             ifelse(order_date_dimension[i,3]=="Saturday",order_date_dimension$type_of_day[i]<-"Weekend",
                    order_date_dimension$type_of_day[i]<-"Weekday"))
}
order_date_dimension$day<-day(order_date_dimension$order_date)
order_date_dimension$month<-month(order_date_dimension$order_date,label=TRUE,abbr=FALSE)
order_date_dimension$year<-year(order_date_dimension$order_date)

for(i in 1:nrow(order_date_dimension)){
      if(order_date_dimension$month[i]=="February" & leap_year(order_date_dimension$year[i])==TRUE){
            end<-29
      }
      else if (order_date_dimension$month[i]=="February" & leap_year(order_date_dimension$year[i])==FALSE){
            end<-28
      }
      else {
            end<-30
      }
      if(order_date_dimension$day[i]==15|order_date_dimension$day[i]==end){
            if(order_date_dimension$type_of_day[i]=="Weekday"){
                  order_date_dimension$payday[i]<-"Yes"
            }
            else if(order_date_dimension$type_of_day[i]=="Weekend"){
                  if (order_date_dimension$type_of_day[i-1]=="Weekday"){
                        order_date_dimension$payday[i-1]<-"Yes"
                        order_date_dimension$payday[i]<-"No"
                  }
                  else {
                        order_date_dimension$payday[i-2]<-"Yes"
                        order_date_dimension$payday[i]<-"No"
                  }
            }
      }
      else{
            order_date_dimension$payday[i]<-"No"
      }
}


#required date dimension
required_date_dimension<-rbind(required_date_dimension,temp)
colnames(required_date_dimension)<-"required_date_key"
required_date_dimension$required_date<-seq(ymd("2010-01-01"),ymd("2019-12-31"),by= "1 day")
required_date_dimension$required_day_of_week<-weekdays(required_date_dimension$required_date)
#check type of day
for (i in 1:nrow(required_date_dimension)){
      ifelse(required_date_dimension[i,3]=="Sunday",required_date_dimension$type_of_day[i]<-"Weekend",
             ifelse(required_date_dimension[i,3]=="Saturday",required_date_dimension$type_of_day[i]<-"Weekend",
                    required_date_dimension$type_of_day[i]<-"Weekday"))
}
required_date_dimension$day<-day(required_date_dimension$required_date)
required_date_dimension$month<-month(required_date_dimension$required_date,label=TRUE,abbr=FALSE)
required_date_dimension$year<-year(required_date_dimension$required_date)

for(i in 1:nrow(required_date_dimension)){
      if(required_date_dimension$month[i]=="February" & leap_year(required_date_dimension$year[i])==TRUE){
            end<-29
      }
      else if (required_date_dimension$month[i]=="February" & leap_year(required_date_dimension$year[i])==FALSE){
            end<-28
      }
      else {
            end<-30
      }
      if(required_date_dimension$day[i]==15|required_date_dimension$day[i]==end){
            if(required_date_dimension$type_of_day[i]=="Weekday"){
                  required_date_dimension$payday[i]<-"Yes"
            }
            else if(required_date_dimension$type_of_day[i]=="Weekend"){
                  if (required_date_dimension$type_of_day[i-1]=="Weekday"){
                        required_date_dimension$payday[i-1]<-"Yes"
                        required_date_dimension$payday[i]<-"No"
                  }
                  else {
                        required_date_dimension$payday[i-2]<-"Yes"
                        required_date_dimension$payday[i]<-"No"
                  }
            }
      }
      else{
            required_date_dimension$payday[i]<-"No"
      }
}


## shipped_date_dimension
temp<-as.data.frame(1:3652)
shipped_date_dimension<-rbind(shipped_date_dimension,temp)
colnames(shipped_date_dimension)<-"shipped_date_key"
shipped_date_dimension$shipped_date<-seq(ymd("2010-01-01"),ymd("2019-12-31"),by= "1 day")
shipped_date_dimension$shipped_day_of_week<-weekdays(shipped_date_dimension$shipped_date)

for (i in 1:nrow(shipped_date_dimension)){
      ifelse(shipped_date_dimension[i,3]=="Sunday",shipped_date_dimension$type_of_day[i]<-"Weekend",
             ifelse(shipped_date_dimension[i,3]=="Saturday",shipped_date_dimension$type_of_day[i]<-"Weekend",
                    shipped_date_dimension$type_of_day[i]<-"Weekday"))
}
shipped_date_dimension$day<-day(shipped_date_dimension$shipped_date)
shipped_date_dimension$month<-month(shipped_date_dimension$shipped_date,label=TRUE,abbr=FALSE)
shipped_date_dimension$year<-year(shipped_date_dimension$shipped_date)

for(i in 1:nrow(shipped_date_dimension)){
      if(shipped_date_dimension$month[i]=="February" & leap_year(shipped_date_dimension$year[i])==TRUE){
            end<-29
      }
      else if (shipped_date_dimension$month[i]=="February" & leap_year(shipped_date_dimension$year[i])==FALSE){
            end<-28
      }
      else {
            end<-30
      }
      if(shipped_date_dimension$day[i]==15|shipped_date_dimension$day[i]==end){
            if(shipped_date_dimension$type_of_day[i]=="Weekday"){
                  shipped_date_dimension$payday[i]<-"Yes"
            }
            else if(shipped_date_dimension$type_of_day[i]=="Weekend"){
                  if (shipped_date_dimension$type_of_day[i-1]=="Weekday"){
                        shipped_date_dimension$payday[i-1]<-"Yes"
                        shipped_date_dimension$payday[i]<-"No"
                  }
                  else {
                        shipped_date_dimension$payday[i-2]<-"Yes"
                        shipped_date_dimension$payday[i]<-"No"
                  }
            }
      }
      else{
            shipped_date_dimension$payday[i]<-"No"
      }
}

#products_dimension
#copy Products into Products_dimension and remove missing values
products_dimension <- Products
for(i in 1:nrow(products_dimension)){
      for(j in 3:ncol(products_dimension)){
            if(is.na(products_dimension[i,j])){
                  product_dimension[i,j]<-"Missing"
            }
      }
}

#sort by ProductName
products_dimension <- products_dimension[order(products_dimension$ProductName),]
#create primary key for customers_dimension
products_dimension$product_key<-1:nrow(products_dimension)
products_dimension <- select(products_dimension, product_key, Id, ProductName, QuantityPerUnit, UnitPrice, UnitsInStock, UnitsOnOrder, ReorderLevel, Discontinued)

#Product_Dimension
products_dimension <- merge(products_dimension, suppliers_dimension, by = "Id", all.x = TRUE)
View(products_dimension)
products_dimension[is.na(products_dimension)] <- "missing"
products_dimension <- select(products_dimension, product_key, Id, ProductName, QuantityPerUnit, UnitPrice, UnitsInStock, UnitsOnOrder, ReorderLevel, Discontinued, supplier_key, CompanyName, ContactName, ContactTitle,
                             Address, City, Region, PostalCode, Country, Phone, Fax, HomePage)

#Sort by ProductName
products_dimension <- products_dimension[order(products_dimension$ProductName),]


#order Transaction dimension
order_transaction_dimension<-Orders
colnames(order_transaction_dimension)[colnames(order_transaction_dimension)=="Id"]<-"OrderId"
for(i in 1:nrow(order_transaction_dimension)){
      if(is.na(order_transaction_dimension[i,6])){
            order_transaction_dimension[i,6]<-order_transaction_dimension[i,5]
      }
}
order_transaction_dimension$DaysDelayed<-ifelse((ymd(order_transaction_dimension$ShippedDate)-ymd(order_transaction_dimension$RequiredDate))<0,
       order_transaction_dimension$DaysDelayed<-0,
       order_transaction_dimension$DaysDelayed<-ymd(order_transaction_dimension$ShippedDate)-ymd(order_transaction_dimension$RequiredDate))
order_transaction_dimension$order_key<-1:nrow(order_transaction_dimension)
order_transaction_dimension<-order_transaction_dimension[,c(16,1:15)]

#Order Details Fact
temp2<-merge(OrderDetails,order_transaction_dimension)
colnames(temp2)[10:12]<-c("order_date","required_date","shipped_date")
temp2$order_date<-ymd(temp2$order_date)
temp2$required_date<-ymd(temp2$required_date)
temp2$shipped_date<-ymd(temp2$shipped_date)
temp3<-left_join(temp2,select(order_date_dimension,order_date,order_date_key),by="order_date")
temp4<-left_join(temp3,select(required_date_dimension,required_date,required_date_key),by="required_date")
temp5<-left_join(temp4,select(shipped_date_dimension,shipped_date,shipped_date_key),by="shipped_date")

colnames(customers_dimension)[2]<-"CustomerId"
temp5<-left_join(temp5,select(customers_dimension,CustomerId,customer_key),by="CustomerId")
for(i in 1:nrow(temp5)){
      if(is.na(temp5[i,25])){
      temp5[i,25]<-"Missing"
      }
}

temp5<-left_join(temp5,select(employees_dimension,Id,employee_key),by=c("EmployeeId"="Id"))
temp5<-left_join(temp5,select(products_dimension,Id,product_key),by=c("ProductId"="Id"))

require( data.table )
temp5 <- data.table( temp5 )
temp5[ , order_item_key := 1:.N , by = c("order_key") ]
temp5 = as.data.frame(temp5)
temp5$order_line_total<-temp5$UnitPrice*temp5$Quantity*(1-temp5$Discount)
final<-temp5[,c("order_key","order_item_key","order_date_key","required_date_key","shipped_date_key","product_key","customer_key","employee_key","UnitPrice","Quantity","Discount","DaysDelayed","order_line_total")]
final$order_details_key<-1:nrow(final)
final<-final[,c(14,1:13)]
order_details_fact<-final

dbWriteTable(con2,"customers_dimension",customers_dimension,overwrite=TRUE)
dbWriteTable(con2,"employees_dimension",employees_dimension,overwrite=TRUE)
dbWriteTable(con2,"products_dimension",products_dimension,overwrite=TRUE)
dbWriteTable(con2,"order_date_dimension",order_date_dimension,overwrite=TRUE)
dbWriteTable(con2,"required_date_dimension",required_date_dimension,overwrite=TRUE)
dbWriteTable(con2,"shipped_date_dimension",shipped_date_dimension,overwrite=TRUE)
dbWriteTable(con2,"order_details_fact",order_details_fact,overwrite=TRUE)

dbDisconnect(con1)
dbDisconnect(con2)
detach("package:data.table",unload=TRUE)
