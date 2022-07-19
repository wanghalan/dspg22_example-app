# if there are datasets to add, include any preprocessing steps here
write.csv(cbind(name = rownames(mtcars), mtcars), "/Users/loremipsum/Documents/GitHub/dspg22_example-app/docs/data/mtcars.csv", row.names = FALSE)

# then add them to the site:
data_add(
  c(mtcars = "mtcars.csv"),
  meta = list(
    ids = list(variable = "name")
  ),
  dir = "/Users/loremipsum/Documents/GitHub/dspg22_example-app/docs/data",
  refresh = TRUE
)

# now edit the site and build it from site.R
