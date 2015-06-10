supported.data.frame =
  function()
    rdata.frame(
      elements =
        mixture(list(rinteger, rdouble, rcharacter, rlogical, rDate)),
      nrow = c(min = 1),
      ncol = c(min = 1))

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
  function(x, y) {
    x = normalize(x)
    y = normalize(y)
    isTRUE(all.equal(x, y))}

equiv.test =
  function(expr.gen){
    test(
      forall(
        x = supported.data.frame(),
        name = dplyr:::random_table_name(),
        src = my_db, {
          names(x) = gsub("\\.", "_", names(x))
          rs = expr.gen(x)
          retval =
            cmp(
              rs(x),
              collect(rs(copy_to(src, x, name))))
          db_drop_table(table = paste0('`', name,'`'), con = src$con)
          retval}),
      about = deparse(substitute(expr.gen)))}
