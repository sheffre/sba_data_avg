#averager: second iter
#trying to parse date and hour from timestap

required_packages <- c("DBI", "RPostgres", "lubridate")

install_if_missing <- function(pkg) {
  if (!requireNamespace(pkg, quietly = TRUE)) {
    install.packages(pkg, dependencies = TRUE)
  }
}

lapply(required_packages, install_if_missing)

library(DBI)
library(RPostgres)
library(lubridate)
library(dplyr)


# conn <- dbConnect(drv = RPostgres::Postgres(),
#                   host     = '81.31.246.77',
#                   user     = 'shinytest',
#                   password = "17082002asxc",
#                   dbname   = "default_db")


query_generator <- function(right_border, left_border) {
  query <- paste0("SELECT co2_partial_pressure, timestamp FROM co2_atm_data WHERE timestamp <= ", "to_timestamp('",
                  right_border, "',  'yyyy-mm-dd hh24:mi:ss') AND timestamp >= ",
                  "to_timestamp('", left_border, "', 'yyyy-mm-dd hh24-mi-ss') ORDER BY timestamp DESC")
  return(query)
}



getter <- function(query) {
  conn <- dbConnect(drv = RPostgres::Postgres(),
                    host     = '81.31.246.77',
                    user     = 'shinytest',
                    password = "17082002asxc",
                    dbname   = "default_db")
  
  df <- dbGetQuery(conn, query)
  
  dbDisconnect(conn)
  return(df)
}

pusher <- function(df){
  conn <- dbConnect(drv = RPostgres::Postgres(),
                    host     = '81.31.246.77',
                    user     = 'shinytest',
                    password = "17082002asxc",
                    dbname   = "default_db")
  
  tryCatch({
    dbAppendTable(conn, "avg_hour_values", df)
    dbDisconnect(conn)
    return(T)
  }, error = function(cond) {
    conn_retry <-  dbConnect(drv = RPostgres::Postgres(),
                             host     = '81.31.246.77',
                             user     = 'retryshinytest',
                             password = "17082002asxcf",
                             dbname   = "default_db")
    tryCatch({
      dbDisconnect(conn)
      dbAppendTable(conn_retry, "avg_hour_value", df)
      dbDisconnect(conn_retry)
      return(T)
    }, error = function(cond) {
      return(F)
    })
    
  })
}

right_border <- as.POSIXct(paste0(as.character(Sys.Date()), " 00:00:00"))
left_border <- as.POSIXct(paste0(as.character(Sys.Date()-1), " 00:00:00"))

query <- query_generator(right_border, left_border)
rm(right_border, left_border)

df <- getter(query)
rm(query)

df$timestamp <- force_tz(df$timestamp, tzone = "Europe/Moscow")

df$hour <- format(as.POSIXct(df$timestamp), format = "%H")

levels(as.factor(df$hour))

avg <- vector()
for (i in levels(as.factor(df$hour))) {
  sub <- mean(subset(df, df$hour == i)$co2_partial_pressure)
  avg <- c(avg, sub)
}


upper_time <- vector()
upper_time[1] <- as.POSIXct(df$timestamp[1])
for(i in c(1:23)) {
  upper_time[i+1] <- upper_time[i]-3600 
}
rm(i)

upper_time[1]
upper_time <- rev(upper_time)

df_append <- as.data.frame(cbind(round(avg), upper_time))
colnames(df_append) <- c("avg_co2_partial_pressure", "upper_time")
df_append$upper_time <- as.POSIXct(df_append$upper_time, origin = "1970-01-01")


factor <- F
while(factor == F) {
  factor <- pusher(df_append)
}
