tpc_h02 <- function(input_func, collect_func = dplyr::collect) {
  ps <- input_func("partsupp") %>% select(ps_partkey, ps_suppkey, ps_supplycost)

  p <- input_func("part") %>%
    select(p_partkey, p_type, p_size, p_mfgr) %>%
    filter(p_size == 15, grepl(".*BRASS$", p_type)) %>%
    select(p_partkey, p_mfgr)

  psp <- inner_join(p, ps, by = c("p_partkey" = "ps_partkey"))

  sp <- input_func("supplier") %>%
    select(s_suppkey, s_nationkey, s_acctbal, s_name,
           s_address, s_phone, s_comment)

  psps <- inner_join(psp, sp,
                     by = c("ps_suppkey" = "s_suppkey")) %>%
    select(p_partkey, ps_supplycost, p_mfgr, s_nationkey,
           s_acctbal, s_name, s_address, s_phone, s_comment)

  nr <- inner_join(input_func("nation"),
                   input_func("region") %>% filter(r_name == "EUROPE"),
                   by = c("n_regionkey" = "r_regionkey")) %>%
    select(n_nationkey, n_name)

  pspsnr <- inner_join(psps, nr,
                       by = c("s_nationkey" = "n_nationkey")) %>%
    select(p_partkey, ps_supplycost, p_mfgr, n_name, s_acctbal,
           s_name, s_address, s_phone, s_comment)

  aggr <- pspsnr %>%
    group_by(p_partkey) %>%
    summarise(min_ps_supplycost = min(ps_supplycost))

  sj <- inner_join(pspsnr, aggr,
                   by=c("p_partkey" = "p_partkey", "ps_supplycost" = "min_ps_supplycost"))


  sj %>%
    select(s_acctbal, s_name, n_name, p_partkey, p_mfgr,
           s_address, s_phone, s_comment) %>%
    arrange(desc(s_acctbal), n_name, s_name, p_partkey) %>%
    head(100) %>%
    collect_func()
}
