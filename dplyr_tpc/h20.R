tpc_h20 <- function(input_func, collect_func = dplyr::collect) {
  supplier_ca <- input_func("supplier") %>%
    inner_join(
      input_func("nation") %>% filter(n_name == "CANADA"),
      by = c("s_nationkey" = "n_nationkey")
    ) %>%
    select(s_suppkey, s_name, s_address)

  part_forest <- input_func("part") %>%
    filter(grepl("^forest", p_name))

  partsupp_forest_ca <- input_func("partsupp") %>%
    semi_join(supplier_ca, c("ps_suppkey" = "s_suppkey")) %>%
    semi_join(part_forest, by = c("ps_partkey" = "p_partkey"))

  qty_threshold <- input_func("lineitem") %>%
    filter(
      l_shipdate >= as.Date("1994-01-01"),
      l_shipdate < as.Date("1995-01-01")
    ) %>%
    semi_join(partsupp_forest_ca, by = c("l_partkey" = "ps_partkey", "l_suppkey" = "ps_suppkey")) %>%
    group_by(l_suppkey) %>%
    summarise(qty_threshold = 0.5 * sum(l_quantity))

  partsupp_forest_ca_filtered <- partsupp_forest_ca %>%
    inner_join(
      qty_threshold,
      by = c("ps_suppkey" = "l_suppkey")
    ) %>%
    filter(ps_availqty > qty_threshold)

  supplier_ca %>%
    semi_join(partsupp_forest_ca_filtered, by = c("s_suppkey" = "ps_suppkey")) %>%
    select(s_name, s_address) %>%
    arrange(s_name) %>%
    collect_func()
}
