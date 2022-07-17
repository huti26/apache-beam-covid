library(ggplot2)
library(svglite)

setwd(dirname(rstudioapi::getActiveDocumentContext()$path))

cbp1 <- c("#999999", "#E69F00", "#56B4E9", "#009E73",
          "#F0E442", "#0072B2", "#D55E00", "#CC79A7")

data <- read.csv("./data/exe4b", sep=",", header = FALSE, col.names = c("Altersgruppe", "Monat","Genesungen"))

monate <- c("Januar","Februar","März","April", "Mai", "Juni", "Juli",
            "August", "September", "Oktober", "November", "Dezember")

g <- ggplot(data, aes(x=Monat,y=Genesungen,fill=Altersgruppe)) +
  geom_bar(stat = "identity", position = "dodge") +
  scale_x_continuous(breaks = 1:12, labels = monate, limits = c(3,12)) +
  scale_fill_manual(values = cbp1) +
  labs(
    title = "Absolute Anzahl an Genesungen pro Altersgruppe pro Monat",
    subtitle = "Keine Genesungen im Januar/Februar vorhanden",
    x = ""
  )
  

ggsave(filename = "4b.svg")

print(g)