tpc_h16 <- function(input_func, collect_func = dplyr::collect) {
  part_filtered <- input_func("part") %>%
    filter(
      p_brand != "Brand#45",
      !grepl("^MEDIUM POLISHED", p_type),
      p_size %in% c(49, 14, 23, 45, 19, 3, 36, 9)
    )

  supplier_filtered <- input_func("supplier") %>%
    filter(!grepl("Customer.*?Complaints", s_comment))

  partsupp_filtered <- input_func("partsupp") %>%
    inner_join(supplier_filtered, by = c("ps_suppkey" = "s_suppkey")) %>%
    select(ps_partkey, ps_suppkey)

  part_filtered %>%
    inner_join(partsupp_filtered, by = c("p_partkey" = "ps_partkey")) %>%
    group_by(p_brand, p_type, p_size) %>%
    summarise(supplier_cnt = n_distinct(ps_suppkey)) %>%
    ungroup() %>%
    select(p_brand, p_type, p_size, supplier_cnt) %>%
    arrange(desc(supplier_cnt), p_brand, p_type, p_size) %>%
    collect_func()
}
