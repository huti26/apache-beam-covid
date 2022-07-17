library(ggplot2)
library(svglite)

setwd(dirname(rstudioapi::getActiveDocumentContext()$path))

data <- read.csv("./data/exe1", sep=";", header = FALSE, col.names = c("Bundesland", "Infektionen"))


g <- ggplot(data, aes(x="", y=Infektionen, fill=Bundesland)) +
  geom_bar(stat = "identity", width = 1, color="white") +
  coord_polar("y", start = 0) +
  theme_void() +
  labs(
    title = "Verteilung der absoluten Infektionzahlen nach Bundesland"
  )
  
ggsave(filename = "1b.svg")

print(g)