tpc_h05 <- function(input_func, collect_func = dplyr::collect) {
  nr <- inner_join(
    input_func("nation") %>%
      select(n_nationkey, n_regionkey, n_name),
    input_func("region") %>%
      select(r_regionkey, r_name) %>%
      filter(r_name == "ASIA"),
    by = c("n_regionkey" = "r_regionkey")
  ) %>%
    select(n_nationkey, n_name)

  snr <- inner_join(
    input_func("supplier") %>%
      select(s_suppkey, s_nationkey),
    nr,
    by = c("s_nationkey" = "n_nationkey")
  ) %>%
    select(s_suppkey, s_nationkey, n_name)

  lsnr <- inner_join(
    input_func("lineitem") %>% select(l_suppkey, l_orderkey, l_extendedprice, l_discount),
    snr, by = c("l_suppkey" = "s_suppkey"))

  o <- input_func("orders") %>%
    select(o_orderdate, o_orderkey, o_custkey) %>%
    # kludge: filter(o_orderdate >= "1994-01-01", o_orderdate < "1994-01-01" + interval '1' year) %>%
    filter(o_orderdate >= as.Date("1994-01-01"), o_orderdate < as.Date("1995-01-01")) %>%
    select(o_orderkey, o_custkey)

  oc <- inner_join(o, input_func("customer") %>% select(c_custkey, c_nationkey),
                   by = c("o_custkey" = "c_custkey")) %>%
    select(o_orderkey, c_nationkey)

  lsnroc <- inner_join(lsnr, oc,
                       by = c("l_orderkey" = "o_orderkey", "s_nationkey" = "c_nationkey")) %>%
    select(l_extendedprice, l_discount, n_name)

  lsnroc %>%
    mutate(volume=l_extendedprice * (1 - l_discount)) %>%
    group_by(n_name) %>%
    summarise(revenue = sum(volume)) %>%
    ungroup() %>%
    arrange(desc(revenue)) %>%
    collect_func()
}
