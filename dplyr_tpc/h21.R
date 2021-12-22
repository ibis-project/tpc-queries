tpc_h21 <- function(input_func, collect_func = dplyr::collect) {
  orders_with_more_than_one_supplier <- input_func("lineitem") %>%
    group_by(l_orderkey) %>%
    count(l_suppkey) %>%
    group_by(l_orderkey) %>%
    summarise(n_supplier = n()) %>%
    filter(n_supplier > 1)

  line_items_needed <- input_func("lineitem") %>%
    semi_join(orders_with_more_than_one_supplier) %>%
    inner_join(input_func("orders"), by = c("l_orderkey" = "o_orderkey")) %>%
    filter(o_orderstatus == "F") %>%
    group_by(l_orderkey, l_suppkey) %>%
    summarise(failed_delivery_commit = any(l_receiptdate > l_commitdate)) %>%
    group_by(l_orderkey) %>%
    summarise(n_supplier = n(), num_failed = sum(failed_delivery_commit)) %>%
    filter(n_supplier > 1 & num_failed == 1)

  line_items <- input_func("lineitem") %>%
    semi_join(line_items_needed)

  input_func("supplier") %>%
    inner_join(line_items, by = c("s_suppkey" = "l_suppkey")) %>%
    filter(l_receiptdate > l_commitdate) %>%
    inner_join(input_func("nation"), by = c("s_nationkey" = "n_nationkey")) %>%
    filter(n_name == "SAUDI ARABIA") %>%
    group_by(s_name) %>%
    summarise(numwait = n()) %>%
    ungroup() %>%
    arrange(desc(numwait), s_name) %>%
    head(100) %>%
    collect_func()
}
