rdplyr_expression =
  function(height){
    sample(rselect, rarrange, rfilter, rmutate, rgroup_by, rsummarize) %>%
  if(height > 0)
    rdplyr_expression(height - 1)
else identity}

somecols =
  function(d.f)
    unname(rsample(colnames(d.f), size = ~sample(1:ncol(d.f), 1), replace = FALSE))

rselect =
  function(d.f){
    cols = somecols(d.f)
    function(x)
      select_(x, .dots = cols)}

rarrange =
  function(d.f) {
    cols = somecols(d.f)
    function(x)
      arrange(x, cols)}

rmutate =
  function(d.f) {
    cols = somecols(d.f)
    function(x)
      mutate(x, cols)}

rfilter =
  function(d.f) {
    cols = somecols(d.f)
    function(x)
      filter(x, cols)}

rsummarize =
  function(d.f) {
    cols = somecols(d.f)
    function(x)
      summarize(x, cols)}

rgroup_by =
  function(d.f) {
    cols = somecols(d.f)
    function(x)
      group_by(x, cols)}

normalize =
  function(d.f) {
    d.f = arrange_(d.f, .dots = colnames(d.f))
    rownames(d.f) = NULL
    as.data.frame(d.f)}

cmp =
  function(d.f, src) {
    d.f = normalize(d.f)
    d.f1 = normalize(collect(src))
    isTRUE(all.equal(d.f, d.f1))}

to = test(
  forall(
    x =
      rdata.frame(
        elements =
          mixture(list(rinteger, rdouble, rcharacter, rlogical, rDate)),
        nrow = c(min = 1),
        ncol = c(min = 1)),
    name = dplyr:::random_table_name(),
    src = my_db, {
      names(x) = paste0("a", bitops::cksum(names(x)))
      rs = rselect(x)
      retval =
        cmp(
          rs(x),
          rs(copy_to(src, x, name)))
      db_drop_table(table = paste0('`', name,'`'), con = src$con)
      retval}))
