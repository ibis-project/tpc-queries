tpc_h11 <- function(input_func, collect_func = dplyr::collect) {
  nation <- input_func("nation") %>%
    filter(n_name == "GERMANY")

  joined_filtered <- input_func("partsupp") %>%
    inner_join(input_func("supplier"), by = c("ps_suppkey" = "s_suppkey")) %>%
    inner_join(nation, by = c("s_nationkey" = "n_nationkey"))

  global_agr <- joined_filtered %>%
    summarise(
      global_value = sum(ps_supplycost * ps_availqty) * 0.0001000000
    ) %>%
    mutate(global_agr_key = 1L)

  partkey_agr <- joined_filtered %>%
    group_by(ps_partkey) %>%
    summarise(value = sum(ps_supplycost * ps_availqty))

  partkey_agr %>%
    mutate(global_agr_key = 1L) %>%
    inner_join(global_agr, by = "global_agr_key") %>%
    filter(value > global_value) %>%
    arrange(desc(value)) %>%
    select(ps_partkey, value) %>%
    collect_func()
}
