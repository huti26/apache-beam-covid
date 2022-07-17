library(ggplot2)
library(svglite)

setwd(dirname(rstudioapi::getActiveDocumentContext()$path))

cbp1 <- c("#999999", "#E69F00", "#56B4E9", "#009E73",
          "#F0E442", "#0072B2", "#D55E00", "#CC79A7")

data <- read.csv("./data/exe2b", sep=",", header = FALSE, col.names = c("Altersgruppe", "Datum","Tode"))

data$Datum <- as.Date(data$Datum, format = "%Y/%m/%d")

g <- ggplot(data, aes(x=Datum,y=Tode,color=Altersgruppe)) + 
  geom_line(size = 1.2) +
  scale_color_manual(values = cbp1) +
  labs(
    title = "Verlauf der absoluten Anzahl an Toden pro Altersgruppe",
    x = ""
  )

ggsave(filename = "2b.svg")

print(g)