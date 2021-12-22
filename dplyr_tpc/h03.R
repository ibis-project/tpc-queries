tpc_h03 <- function(input_func, collect_func = dplyr::collect) {
  oc <- inner_join(
    input_func("orders") %>%
      select(o_orderkey, o_custkey, o_orderdate, o_shippriority) %>%
      # kludge, should be: filter(o_orderdate < "1995-03-15"),
      filter(o_orderdate < as.Date("1995-03-15")),
    input_func("customer") %>%
      select(c_custkey, c_mktsegment) %>%
      filter(c_mktsegment == "BUILDING"),
    by = c("o_custkey" = "c_custkey")
  ) %>%
    select(o_orderkey, o_orderdate, o_shippriority)

  loc <- inner_join(
    input_func("lineitem") %>%
      select(l_orderkey, l_shipdate, l_extendedprice, l_discount) %>%
      filter(l_shipdate > as.Date("1995-03-15")) %>%
      select(l_orderkey, l_extendedprice, l_discount),
    oc, by = c("l_orderkey" = "o_orderkey")
  )

  loc %>% mutate(volume=l_extendedprice * (1 - l_discount)) %>%
    group_by(l_orderkey, o_orderdate, o_shippriority) %>%
    summarise(revenue = sum(volume)) %>%
    select(l_orderkey, revenue, o_orderdate, o_shippriority) %>%
    ungroup() %>%
    arrange(desc(revenue), o_orderdate) %>%
    head(10) %>%
    collect_func()
}
