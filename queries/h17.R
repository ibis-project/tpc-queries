tpc_h17 <- function(input_func, collect_func = dplyr::collect) {
  parts_filtered <- input_func("part") %>%
    filter(
      p_brand == "Brand#23",
      p_container == "MED BOX"
    )

  joined <- input_func("lineitem") %>%
    inner_join(parts_filtered, by = c("l_partkey" = "p_partkey"))

  quantity_by_part <- joined %>%
    group_by(l_partkey) %>%
    summarise(quantity_threshold = 0.2 * mean(l_quantity))

  joined %>%
    inner_join(quantity_by_part, by = "l_partkey") %>%
    filter(l_quantity < quantity_threshold) %>%
    summarise(avg_yearly = sum(l_extendedprice) / 7.0) %>%
    collect_func()
}
