tpc_h04 <- function(input_func, collect_func = dplyr::collect) {
  l <- input_func("lineitem") %>%
    select(l_orderkey, l_commitdate, l_receiptdate) %>%
    filter(l_commitdate < l_receiptdate) %>%
    select(l_orderkey)

  o <- input_func("orders") %>%
    select(o_orderkey, o_orderdate, o_orderpriority) %>%
    # kludge: filter(o_orderdate >= "1993-07-01", o_orderdate < "1993-07-01" + interval '3' month) %>%
    filter(o_orderdate >= as.Date("1993-07-01"), o_orderdate < as.Date("1993-10-01")) %>%
    select(o_orderkey, o_orderpriority)

  # distinct after join, tested and indeed faster
  lo <- inner_join(l, o, by = c("l_orderkey" = "o_orderkey")) %>%
    distinct() %>%
    select(o_orderpriority)

  lo %>%
    group_by(o_orderpriority) %>%
    summarise(order_count = n()) %>%
    ungroup() %>%
    arrange(o_orderpriority) %>%
    collect_func()
}
