
library(dplyr, warn.conflicts = FALSE)
library(lubridate, warn.conflicts = FALSE)
library(DBI)

run_query <- function(qfunc, dbfn) {
  con <- dbConnect(RSQLite::SQLite(), dbfn)

  db_table <- function(name) {
    return(dbReadTable(con, name))
  }

  res <- qfunc(db_table)

  # clean up database file
  DBI::dbDisconnect(con, shutdown = TRUE)

  return(as.data.frame(res))
}
