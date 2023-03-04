tpc_h14 <- function(input_func, collect_func = dplyr::collect) {
  input_func("lineitem") %>%
    filter(
      l_shipdate >= as.Date("1995-09-01"),
      l_shipdate < as.Date("1995-10-01")
    ) %>%
    inner_join(input_func("part"), by = c("l_partkey" = "p_partkey")) %>%
    summarise(
      promo_revenue = 100 * sum(
        if_else(grepl("^PROMO", p_type), l_extendedprice * (1 - l_discount), 0)
      ) / sum(l_extendedprice * (1 - l_discount))
    ) %>%
    collect_func()
}
