tpc_h10 <- function(input_func, collect_func = dplyr::collect) {
  l <- input_func("lineitem") %>%
    select(l_orderkey, l_returnflag, l_extendedprice, l_discount) %>%
    filter(l_returnflag == "R") %>%
    select(l_orderkey, l_extendedprice, l_discount)

  o <- input_func("orders") %>%
    select(o_orderkey, o_custkey, o_orderdate) %>%
    # kludge, filter(o_orderdate >= "1993-10-01", o_orderdate < "1994-01-01") %>%
    filter(o_orderdate >= as.Date("1993-10-01"), o_orderdate < as.Date("1994-01-01")) %>%
    select(o_orderkey, o_custkey)

  lo <- inner_join(l, o,
                   by = c("l_orderkey" = "o_orderkey")) %>%
    select(l_extendedprice, l_discount, o_custkey)
  # first aggregate, then join with customer/nation,
  # otherwise we need to aggr over lots of cols

  lo_aggr <- lo %>% mutate(volume=l_extendedprice * (1 - l_discount)) %>%
    group_by(o_custkey) %>%
    summarise(revenue = sum(volume))

  c <- input_func("customer") %>%
    select(c_custkey, c_nationkey, c_name, c_acctbal, c_phone, c_address, c_comment)

  loc <- inner_join(c, lo_aggr, by = c("c_custkey" = "o_custkey"))

  locn <- inner_join(loc, input_func("nation") %>% select(n_nationkey, n_name),
                     by = c("c_nationkey" = "n_nationkey"))

  locn %>%
    select(c_custkey, c_name, revenue, c_acctbal, n_name,
           c_address, c_phone, c_comment) %>%
    arrange(desc(revenue)) %>%
    head(20) %>%
    collect_func()
}
