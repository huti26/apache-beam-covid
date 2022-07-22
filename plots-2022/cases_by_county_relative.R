library(ggplot2)
library(RColorBrewer)

options(scipen = 10000)

setwd(dirname(rstudioapi::getActiveDocumentContext()$path))

data <-
  read.csv(
    "./data/inhabitants_per_county.csv",
    sep = ",",
  )


getPalette = colorRampPalette(brewer.pal(9, "Set1"))

data$relative.cases = data$cases / data$inhabitants
data$per.100k.capita = data$relative.cases * 1000000

# g <-
#   ggplot(data, aes(
#     x = reorder(county, cases, decreasing = TRUE),
#     y = per.100k.capita
#   )) +
#   geom_bar(
#     stat = "identity",
#     width = 1,
#     color = "white",
#     position = "dodge"
#   ) +
#   labs(
#     title = "Infections per Million People per County",
#     subtitle = "Germany - 17.07.2022",
#     x = "",
#     y = "Infections"
#   ) +
#   theme(axis.text.x = element_text(
#     angle = 90,
#     vjust = 0.5,
#     hjust = 1
#   ))
# 
# 
# 
# print(g)

# g <-
#   ggplot(data, aes(
#     x = reorder(county, cases, decreasing = TRUE),
#     y = relative.cases
#   )) +
#   geom_bar(
#     stat = "identity",
#     width = 1,
#     color = "white",
#     position = "dodge"
#   ) +
#   labs(
#     title = "Relative number of Infections per County",
#     subtitle = "Germany - 17.07.2022",
#     x = "",
#     y = "Relative Infections"
#   ) +
#   theme(axis.text.x = element_text(
#     angle = 90,
#     vjust = 0.5,
#     hjust = 1),
#     text = element_text(size = 14),
#     plot.margin = margin(t = 1, r = 2, b = 1, l = 1, unit = "cm"
#   ))
# 
# 
# 
# print(g)
# 
# ggsave("cases_by_county_relative.png", width = 8.37, height = 6, dpi = 300)

g <-
  ggplot(data, aes(
    y = reorder(county, cases, decreasing = FALSE),
    x = relative.cases
  )) +
  geom_bar(
    stat = "identity",
    width = 1,
    color = "white",
    position = "dodge"
  ) +
  labs(
    title = "Relative number of Infections per County",
    subtitle = "Germany - 17.07.2022",
    x = "Relative Infections",
    y = ""
  ) +
  theme(
    text = element_text(size = 14),
    plot.margin = margin(t = 0.5, r = 1, b = 0.5, l = 0, unit = "cm"
    ))



print(g)

ggsave("cases_by_county_relative.png", width = 8.37, height = 6, dpi = 300)

