tpc_h08 <- function(input_func, collect_func = dplyr::collect) {
  # kludge, swapped the table order around because of ARROW-14184
  # nr <- inner_join(
  #   input_func("nation") %>%
  #     select(n1_nationkey = n_nationkey, n1_regionkey = n_regionkey),
  #   input_func("region") %>%
  #     select(r_regionkey, r_name) %>%
  #     filter(r_name == "AMERICA") %>%
  #     select(r_regionkey),
  #   by = c("n1_regionkey" = "r_regionkey")) %>%
  #   select(n1_nationkey)
  nr <- inner_join(
    input_func("region") %>%
      select(r_regionkey, r_name) %>%
      filter(r_name == "AMERICA") %>%
      select(r_regionkey),
    input_func("nation") %>%
      select(n1_nationkey = n_nationkey, n1_regionkey = n_regionkey),
    by = c("r_regionkey" = "n1_regionkey")) %>%
    select(n1_nationkey)

  cnr <- inner_join(
    input_func("customer") %>%
      select(c_custkey, c_nationkey),
    nr, by = c("c_nationkey" = "n1_nationkey")) %>%
    select(c_custkey)

  ocnr <- inner_join(
    input_func("orders") %>%
      select(o_orderkey, o_custkey, o_orderdate) %>%
      # bludge, should be: filter(o_orderdate >= "1995-01-01", o_orderdate <= "1996-12-31"),
      filter(o_orderdate >= as.Date("1995-01-01"), o_orderdate <= as.Date("1996-12-31")),
    cnr, by = c("o_custkey" = "c_custkey")) %>%
    select(o_orderkey, o_orderdate)

  locnr <- inner_join(
    input_func("lineitem") %>%
      select(l_orderkey, l_partkey, l_suppkey, l_extendedprice, l_discount),
    ocnr, by=c("l_orderkey" = "o_orderkey")) %>%
    select(l_partkey, l_suppkey, l_extendedprice, l_discount, o_orderdate)

  locnrp <- inner_join(locnr,
                       input_func("part") %>%
                         select(p_partkey, p_type) %>%
                         filter(p_type == "ECONOMY ANODIZED STEEL") %>%
                         select(p_partkey),
                       by = c("l_partkey" = "p_partkey")) %>%
    select(l_suppkey, l_extendedprice, l_discount, o_orderdate)

  locnrps <- inner_join(locnrp,
                        input_func("supplier") %>%
                          select(s_suppkey, s_nationkey),
                        by = c("l_suppkey" = "s_suppkey")) %>%
    select(l_extendedprice, l_discount, o_orderdate, s_nationkey)

  all <- inner_join(locnrps,
                    input_func("nation") %>%
                      select(n2_nationkey = n_nationkey, n2_name = n_name),
                    by = c("s_nationkey" = "n2_nationkey")) %>%
    select(l_extendedprice, l_discount, o_orderdate, n2_name)

  all %>%
    mutate(
      # kludge(?), o_year = as.integer(strftime(o_orderdate, "%Y")),
      o_year = year(o_orderdate),
      volume = l_extendedprice * (1 - l_discount),
      nation = n2_name) %>%
    select(o_year, volume, nation) %>%
    group_by(o_year) %>%
    summarise(mkt_share = sum(ifelse(nation == "BRAZIL", volume, 0)) / sum(volume)) %>%
    ungroup() %>%
    arrange(o_year) %>%
    collect_func()
}
