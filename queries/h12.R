tpc_h12 <- function(input_func, collect_func = dplyr::collect) {
  input_func("lineitem") %>%
    filter(
      l_shipmode %in% c("MAIL", "SHIP"),
      l_commitdate < l_receiptdate,
      l_shipdate < l_commitdate,
      l_receiptdate >= as.Date("1994-01-01"),
      l_receiptdate < as.Date("1995-01-01")
    ) %>%
    inner_join(
      input_func("orders"),
      by = c("l_orderkey" = "o_orderkey")
    ) %>%
    group_by(l_shipmode) %>%
    summarise(
      high_line_count = sum(
        if_else(
          (o_orderpriority == "1-URGENT") | (o_orderpriority == "2-HIGH"),
          1L,
          0L
        )
      ),
      low_line_count = sum(
        if_else(
          (o_orderpriority != "1-URGENT") & (o_orderpriority != "2-HIGH"),
          1L,
          0L
        )
      )
    ) %>%
    ungroup() %>%
    arrange(l_shipmode) %>%
    collect_func()
}
