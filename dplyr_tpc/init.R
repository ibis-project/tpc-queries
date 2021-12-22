
library(dplyr, warn.conflicts = FALSE)
library(dbplyr, warn.conflicts = FALSE)
library(lubridate, warn.conflicts = FALSE)
library(DBI)

setup_sqlite <- function(dbfn) {
    DBI::dbConnect(RSQLite::SQLite(), dbfn)
}

teardown_sqlite <- function(con) {
  DBI::dbDisconnect(con, shutdown = TRUE)
}

query_dplyr <- function(con, qfunc) {
  db_table <- function(name) {
    dbReadTable(con, name) %>% rename_with(tolower)
  }

  res <- qfunc(db_table)

  return(as.data.frame(res))
}

query_dbplyr <- function(con, qfunc) {
  db_table <- function(name) {
    tbl(con, name) %>% rename_with(tolower)
  }

  res <- qfunc(db_table)

  return(as.data.frame(res))
}

query_sql <- function(con, qfunc) {
  db_table <- function(name) {
    tbl(con, name) %>% rename_with(tolower)
  }

  res <- qfunc(db_table, sql_render)

  return(as.character(res))
}
