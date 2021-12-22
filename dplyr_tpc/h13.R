tpc_h13 <- function(input_func, collect_func = dplyr::collect) {
  c_orders <- input_func("customer") %>%
    left_join(
      input_func("orders") %>%
        filter(!grepl("special.*?requests", o_comment)),
      by = c("c_custkey" = "o_custkey")
    ) %>%
    group_by(c_custkey) %>%
    summarise(
      c_count = sum(!is.na(o_orderkey))
    )

  c_orders %>%
    group_by(c_count) %>%
    summarise(custdist = n()) %>%
    ungroup() %>%
    arrange(desc(custdist), desc(c_count)) %>%
    collect_func()
}
