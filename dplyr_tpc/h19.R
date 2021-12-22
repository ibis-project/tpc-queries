tpc_h19 <- function(input_func, collect_func = dplyr::collect) {
  joined <- input_func("lineitem") %>%
    inner_join(input_func("part"), by = c("l_partkey" = "p_partkey"))

  result <- joined %>%
    filter(
      (
        p_brand == "Brand#12" &
          p_container %in% c('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') &
          l_quantity >= 1 &
          l_quantity <= (1 + 10) &
          p_size >= 1 &
          p_size <= 5 &
          l_shipmode %in% c("AIR", "AIR REG") &
          l_shipinstruct == "DELIVER IN PERSON"
      ) |
        (
          p_brand == "Brand#23" &
            p_container %in% c('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') &
            l_quantity >= 10 &
            l_quantity <= (10 + 10) &
            p_size >= 1 &
            p_size <= 10 &
            l_shipmode %in% c("AIR", "AIR REG") &
            l_shipinstruct == "DELIVER IN PERSON"
        ) |
        (
          p_brand == "Brand#34" &
            p_container %in% c('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') &
            l_quantity >= 20 &
            l_quantity <= (20 + 10) &
            p_size >= 1 &
            p_size <= 15 &
            l_shipmode %in% c("AIR", "AIR REG") &
            l_shipinstruct == "DELIVER IN PERSON"
        )
    )

  result %>%
    summarise(
      revenue = sum(l_extendedprice * (1 - l_discount))
    ) %>%
    collect_func()
}
