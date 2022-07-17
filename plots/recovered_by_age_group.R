library(ggplot2)

setwd(dirname(rstudioapi::getActiveDocumentContext()$path))

cbp1 <- c("#999999", "#E69F00", "#56B4E9", "#009E73",
          "#F0E442", "#0072B2", "#D55E00", "#CC79A7")

data <- read.csv("./data/recovered_by_age_group.csv", sep=",", header = FALSE, col.names = c("Altersgruppe", "Monat","Genesungen"))

monate <- c("January","February","March","April", "May", "June", "July",
            "August", "September", "October", "November", "December")

g <- ggplot(data, aes(x=Monat,y=Genesungen,fill=Altersgruppe)) +
  geom_bar(stat = "identity", position = "dodge") +
  scale_x_continuous(breaks = 1:12, labels = monate, limits = c(3,12)) +
  scale_fill_manual(values = cbp1, labels = c("A00 - A04", "A05 - A14", "A15 - A34", "A35 - A59", "A60 - A79", "A80+", "unknown" )) +
  labs(
    title = "Absolute number of recoveries per age group and month",
    subtitle = "Germany - December 2020 - No recoveries for January and February",
    x = "",
    y = "Recoveries",
    fill = "Age Group"
  )
  

# ggsave(filename = "4b.svg")

print(g)

ggsave("recovered_by_age_group.png", width = 8.37, height = 6, dpi = 300)
