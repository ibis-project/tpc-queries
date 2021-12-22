tpc_h22 <- function(input_func, collect_func = dplyr::collect) {
  acctbal_mins <- input_func("customer") %>%
    filter(
      substr(c_phone, 1, 2) %in% c("13", "31", "23", "29", "30", "18", "17") &
        c_acctbal > 0
    ) %>%
    summarise(acctbal_min = mean(c_acctbal, na.rm = TRUE), join_id = 1L)

  input_func("customer") %>%
    mutate(cntrycode = substr(c_phone, 1, 2), join_id = 1L) %>%
    left_join(acctbal_mins, by = "join_id") %>%
    filter(
      cntrycode %in% c("13", "31", "23", "29", "30", "18", "17") &
        c_acctbal > acctbal_min
    ) %>%
    anti_join(input_func("orders"), by = c("c_custkey" = "o_custkey")) %>%
    select(cntrycode, c_acctbal) %>%
    group_by(cntrycode) %>%
    summarise(
      numcust = n(),
      totacctbal = sum(c_acctbal)
    ) %>%
    ungroup() %>%
    arrange(cntrycode) %>%
    collect_func()
}
