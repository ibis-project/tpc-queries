tpc_h06 <- function(input_func, collect_func = dplyr::collect) {
  input_func("lineitem") %>%
    select(l_shipdate, l_extendedprice, l_discount, l_quantity) %>%
    # kludge, should be: filter(l_shipdate >= "1994-01-01",
    filter(l_shipdate >= as.Date("1994-01-01"),
           # kludge: should be: l_shipdate < "1994-01-01" + interval '1' year,
           l_shipdate < as.Date("1995-01-01"),
           # Should be the following, but https://issues.apache.org/jira/browse/ARROW-14125
           # Need to round because 0.06 - 0.01 != 0.05
           l_discount >= round(0.06 - 0.01, digits = 2),
           l_discount <= round(0.06 + 0.01, digits = 2),
           # l_discount >= 0.05,
           # l_discount <= 0.07,
           l_quantity < 24) %>%
    select(l_extendedprice, l_discount) %>%
    summarise(revenue = sum(l_extendedprice * l_discount)) %>%
    collect_func()
}
