tpc_h01 <- function(input_func, collect_func = dplyr::collect) {
    res <- input_func("lineitem") %>%
    select(L_SHIPDATE, L_RETURNFLAG, L_LINESTATUS, L_QUANTITY,
           L_EXTENDEDPRICE, L_DISCOUNT, L_TAX) %>%
    # kludge, should be: filter(l_shipdate <= "1998-12-01" - interval x day) %>%
    # where x is between 60 and 120, 90 is the only one that will validate.
    filter(L_SHIPDATE <= as.Date("1998-09-02")) %>%
    select(L_RETURNFLAG, L_LINESTATUS, L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX) %>%
    group_by(L_RETURNFLAG, L_LINESTATUS) %>%
    summarise(
      sum_qty = sum(L_QUANTITY),
      sum_base_price = sum(L_EXTENDEDPRICE),
      sum_disc_price = sum(L_EXTENDEDPRICE * (1 - L_DISCOUNT)),
      sum_charge = sum(L_EXTENDEDPRICE * (1 - L_DISCOUNT) * (1 + L_TAX)),
      avg_qty = mean(L_QUANTITY),
      avg_price = mean(L_EXTENDEDPRICE),
      avg_disc = mean(L_DISCOUNT),
      count_order = n()
    ) %>%
    ungroup() %>%
    arrange(L_RETURNFLAG, L_LINESTATUS) %>%
    collect_func()
    return(res)
}
