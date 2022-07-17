library(ggplot2)

setwd(dirname(rstudioapi::getActiveDocumentContext()$path))

cbp1 <- c("#999999", "#E69F00", "#56B4E9", "#009E73",
          "#F0E442", "#0072B2", "#D55E00", "#CC79A7")

data <- read.csv("./data/deaths_by_age_group.csv", sep=",", header = FALSE, col.names = c("Altersgruppe", "Datum","Tode"))

data$Datum <- as.Date(data$Datum, format = "%Y/%m/%d")

g <- ggplot(data, aes(x=Datum,y=Tode,color=Altersgruppe)) + 
  geom_line(size = 1.2) +
  scale_color_manual(values = cbp1, labels = c("A00 - A04", "A05 - A14", "A15 - A34", "A35 - A59", "A60 - A79", "A80+", "unknown" )) +
  labs(
    title = "Development of the absolute number of deaths per age group",
    subtitle = "Germany - December 2020",
    y = "Deaths",
    x = "",
    color = "Age Group"
  )


print(g)

# ggsave("deaths_by_age_group.png", width = 8.37, height = 6, dpi = 300)
