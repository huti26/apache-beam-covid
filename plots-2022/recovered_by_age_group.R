library(ggplot2)
library(tidyverse)
library(lubridate)

setwd(dirname(rstudioapi::getActiveDocumentContext()$path))

cbp1 <- c("#999999", "#E69F00", "#56B4E9", "#009E73",
          "#F0E442", "#0072B2", "#D55E00", "#CC79A7")

data <- read.csv("./data/sum_recovered_by_age_group_and_day.csv", sep=",", header = FALSE, col.names = c("Altersgruppe", "Datum","Genesungen"))

data$Datum <- as.Date(data$Datum, format = "%Y/%m/%d")

# data <- data %>% 
#   group_by(c(Altersgruppe,month = floor_date(Datum, unit = "month"))) %>%
#   summarize(sum_genesungen = sum(Genesungen))


# g <- ggplot(data, aes(x=Datum,y=Genesungen,fill=Altersgruppe)) +
#   geom_bar(stat = "identity", position = "dodge") +
#   scale_fill_manual(values = cbp1, labels = c("A00 - A04", "A05 - A14", "A15 - A34", "A35 - A59", "A60 - A79", "A80+", "unknown" )) +
#   labs(
#     title = "Absolute Number of Recoveries per Age Group and Day",
#     subtitle = "Germany - 17.07.2022",
#     x = "",
#     y = "Recoveries",
#     fill = "Age Group"
#   )
# 
# 
# print(g)

# ggsave("recovered_by_age_group.png", width = 8.37, height = 6, dpi = 300)

age_group_names <- c(
  `A00-A04` = "A00 - A04",
  `A05-A14` = "A05 - A14",
  `A15-A34` = "A15 - A34",
  `A35-A59` = "A35 - A59",
  `A60-A79` = "A60 - A79",
  `A80+` = "A80+",
  `unbekannt` = "unknown"
)

g <- ggplot(data, aes(x=Datum,y=Genesungen,fill=Altersgruppe)) +
  geom_bar(stat = "identity", position = "dodge") +
  facet_wrap(~Altersgruppe, labeller = as_labeller(age_group_names)) +
  scale_fill_manual(values = cbp1, labels = c("A00 - A04", "A05 - A14", "A15 - A34", "A35 - A59", "A60 - A79", "A80+", "unknown" )) +
  labs(
    title = "Absolute Number of Recoveries per Age Group and Day",
    subtitle = "Germany - 17.07.2022",
    x = "",
    y = "Recoveries",
    fill = "Age Group"
  ) +
  guides(
    fill = "none"
  )+
  theme(
    text = element_text(size = 14),
    plot.margin = margin(t = 1, r = 2, b = 1, l = 1, unit = "cm"
    ))


print(g)

ggsave("recovered_by_age_group.png", width = 8.37, height = 6, dpi = 300)

g <- ggplot(data, aes(x=Datum,y=Genesungen,fill=Altersgruppe)) +
  geom_bar(stat = "identity", position = "dodge") +
  facet_wrap(~Altersgruppe, labeller = as_labeller(age_group_names), scales = "free_y") +
  scale_fill_manual(values = cbp1, labels = c("A00 - A04", "A05 - A14", "A15 - A34", "A35 - A59", "A60 - A79", "A80+", "unknown" )) +
  labs(
    title = "Absolute Number of Recoveries per Age Group and Day",
    subtitle = "Germany - 17.07.2022",
    x = "",
    y = "Recoveries",
    fill = "Age Group"
  ) +
  guides(
    fill = "none"
  ) +
  theme(
    text = element_text(size = 14),
    plot.margin = margin(t = 1, r = 2, b = 1, l = 1, unit = "cm"
    ))




print(g)

ggsave("recovered_by_age_group_scales_free.png", width = 8.37, height = 6, dpi = 300)

