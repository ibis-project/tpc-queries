tpc_h18 <- function(input_func, collect_func = dplyr::collect) {
  big_orders <- input_func("lineitem") %>%
    group_by(l_orderkey) %>%
    summarise(`sum(l_quantity)` = sum(l_quantity)) %>%
    filter(`sum(l_quantity)` > 300)

  input_func("orders") %>%
    inner_join(big_orders, by = c("o_orderkey" = "l_orderkey")) %>%
    inner_join(input_func("customer"), by = c("o_custkey" = "c_custkey")) %>%
    select(
      c_name, c_custkey = o_custkey, o_orderkey,
      o_orderdate, o_totalprice, `sum(l_quantity)`
    ) %>%
    arrange(desc(o_totalprice), o_orderdate) %>%
    head(100) %>%
    collect_func()
}
