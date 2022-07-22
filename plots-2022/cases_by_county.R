library(ggplot2)
library(RColorBrewer)

options(scipen = 10000)

setwd(dirname(rstudioapi::getActiveDocumentContext()$path))

data <-
  read.csv(
    "./data/cases_by_county.csv",
    sep = ";",
    header = FALSE,
    col.names = c("Bundesland", "Infektionen")
  )


getPalette = colorRampPalette(brewer.pal(9, "Set1"))

data$Infektionen.millionen = data$Infektionen / 1000000

# g <- ggplot(data, aes(x="", y=Infektionen, fill=Bundesland)) +
#   geom_bar(stat = "identity", width = 1, color="white") +
#   coord_polar("y", start = 0) +
#   theme_void() +
#   labs(
#     title = "Verteilung der absoluten Infektionzahlen nach Bundesland"
#   )


# g <-
#   ggplot(data, aes(
#     x = reorder(Bundesland, Infektionen, decreasing = TRUE),
#     y = Infektionen.millionen
#   )) +
#   geom_bar(
#     stat = "identity",
#     width = 1,
#     color = "white",
#     position = "dodge"
#   ) +
#   labs(
#     title = "Absolute number of Infections per County",
#     subtitle = "Germany - 17.07.2022",
#     x = "",
#     y = "Million Infections"
#   ) +
#   theme(axis.text.x = element_text(
#     angle = 90,
#     vjust = 0.5,
#     hjust = 1),
#     text = element_text(size = 14),
#     plot.margin = margin(t = 1, r = 2, b = 1, l = 1, unit = "cm"
#     ))
# 
# 
# 
# print(g)

# ggsave("cases_by_county.png", width = 8.37, height = 6, dpi = 300)

g <-
  ggplot(data, aes(
    y = reorder(Bundesland, Infektionen, decreasing = FALSE),
    x = Infektionen.millionen
  )) +
  geom_bar(
    stat = "identity",
    width = 1,
    color = "white",
    position = "dodge"
  ) +
  labs(
    title = "Absolute number of Infections per County",
    subtitle = "Germany - 17.07.2022",
    x = "Million Infections",
    y = ""
  ) +
  theme(
    text = element_text(size = 14),
    plot.margin = margin(t = 0.5, r = 1, b = 0.5, l = 0, unit = "cm"
    ))



print(g)

ggsave("cases_by_county.png", width = 8.37, height = 6, dpi = 300)
