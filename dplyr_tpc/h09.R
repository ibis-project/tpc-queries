tpc_h09 <- function(input_func, collect_func = dplyr::collect) {
  p <- input_func("part") %>%
    select(p_name, p_partkey) %>%
    filter(grepl(".*green.*", p_name)) %>%
    select(p_partkey)

  psp <- inner_join(
    input_func("partsupp") %>%
      select(ps_suppkey, ps_partkey, ps_supplycost),
    p, by = c("ps_partkey" = "p_partkey"))

  sn <- inner_join(
    input_func("supplier") %>%
      select(s_suppkey, s_nationkey),
    input_func("nation") %>%
      select(n_nationkey, n_name),
    by = c("s_nationkey" = "n_nationkey")) %>%
    select(s_suppkey, n_name)

  pspsn <- inner_join(psp, sn, by = c("ps_suppkey" = "s_suppkey"))

  lpspsn <- inner_join(
    input_func("lineitem") %>%
      select(l_suppkey, l_partkey, l_orderkey, l_extendedprice, l_discount, l_quantity),
    pspsn,
    by = c("l_suppkey" = "ps_suppkey", "l_partkey" = "ps_partkey")) %>%
    select(l_orderkey, l_extendedprice, l_discount, l_quantity, ps_supplycost, n_name)

  all <- inner_join(
    input_func("orders") %>%
      select(o_orderkey, o_orderdate),
    lpspsn,
    by = c("o_orderkey"= "l_orderkey" )) %>%
    select(l_extendedprice, l_discount, l_quantity, ps_supplycost, n_name, o_orderdate)

  all %>%
    mutate(
      nation = n_name,
      # kludge, o_year = as.integer(format(o_orderdate, "%Y")),
      # also ARROW-14200
      o_year = year(o_orderdate),
      amount = l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity) %>%
    select(nation, o_year, amount) %>%
    group_by(nation, o_year) %>%
    summarise(sum_profit = sum(amount)) %>%
    ungroup() %>%
    arrange(nation, desc(o_year)) %>%
    collect_func()
}
