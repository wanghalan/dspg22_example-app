library(devtools)
install('/Users/loremipsum/Documents/GitHub/dspg22_community')

library(community)

output <- "/Users/loremipsum/Documents/GitHub/dspg22_example-app"

page_navbar(
  title="Alan's example community-based app", 
  logo = "https://yaoeh.github.io/assets/img/AW.svg",
  list(
    name = "Settings",
    backdrop = "false",
    scale = TRUE,
    class = "menu-compact",
    items = list(
      input_switch("Dark Theme", id = "settings.theme_dark"),
      input_switch(
        "Color by Rank", id = "settings.color_by_order",
        note = paste(
          "Switch from coloring by value to coloring by sorted index.",
          "This may help differentiate regions with similar values."
        )
      ),
      # If you don't include the clear settings input button, an error is thrown
      input_button("Clear Settings", "reset_storage", "clear_storage", class = "btn-danger footer")
    )
  ),
  list(
    name = "About",
    items = list(
      page_text(c(
        paste0(
          "This site was made by the [Social and Decision Analytics Division]",
          "(https://biocomplexity.virginia.edu/institute/divisions/social-and-decision-analytics)",
          " of the [Biocomplexity Institute](https://biocomplexity.virginia.edu)."
        ),
        "View its source on [GitHub](https://github.com/yaoeh/dspg22_example-app).",
        input_button("Download All Data", "export", query = list(
          features = list(geoid = "id", name = "name")
        ), class = "btn-full"),
        "Credits",
        paste(
          "Built in [R](https://www.r-project.org) with the",
          "[community](https://uva-bi-sdad.github.io/community) package, using these resources:"
        )
      ), class = c("", "", "h5")),
      output_credits()
    )
  )
)

 
page_menu(
  input_select("X Variable:", "variables", default = "x", id = "selected_x"),
  default_open = TRUE
)
page_section(
  wraps = "col",
  output_plot("selected_x", "y", "z")
)

# For testing:
site_build(output, open_after = TRUE, serve = TRUE , version ="local")

# For publishing
# site_build(output, open_after = TRUE, serve = TRUE)

