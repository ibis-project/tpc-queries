tpc_h01 <- function(input_func, collect_func = dplyr::collect) {
    input_func("lineitem") %>%
    select(l_shipdate, l_returnflag, l_linestatus, l_quantity,
           l_extendedprice, l_discount, l_tax) %>%
    # kludge, should be: filter(l_shipdate <= "1998-12-01" - interval x day) %>%
    # where x is between 60 and 120, 90 is the only one that will validate.
    filter(l_shipdate <= as.Date("1998-09-01")) %>%
    select(l_returnflag, l_linestatus, l_quantity, l_extendedprice, l_discount, l_tax) %>%
    group_by(l_returnflag, l_linestatus) %>%
    summarise(
      sum_qty = sum(l_quantity),
      sum_base_price = sum(l_extendedprice),
      sum_disc_price = sum(l_extendedprice * (1 - l_discount)),
      sum_charge = sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)),
      avg_qty = mean(l_quantity),
      avg_price = mean(l_extendedprice),
      avg_disc = mean(l_discount),
      count_order = n(),
      .groups = "drop"
    ) %>%
    ungroup() %>%
    arrange(l_returnflag, l_linestatus) %>%
    collect_func()
}
