library(community)

page_navbar("Site Title")
output_plot("x", "y", "z")

site_build(output, open_after = TRUE, serve = TRUE)
