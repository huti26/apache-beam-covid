library(formattable)
# library("htmltools")
# library("webshot")    
# 
# export_formattable <- function(f, file, width = "100%", height = NULL, 
#                                background = "white", delay = 0.2)
# {
#   w <- as.htmlwidget(f, width = width, height = height)
#   path <- html_print(w, background = background, viewer = NULL)
#   url <- paste0("file:///", gsub("\\\\", "/", normalizePath(path)))
#   webshot(url,
#           file = file,
#           selector = ".formattable_widget",
#           delay = delay)
# }
# export_formattable(data_table, "test.png")

options(scipen = 10000)

setwd(dirname(rstudioapi::getActiveDocumentContext()$path))

data <-
  read.csv(
    "./data/10_days_with_most_deaths.csv",
    sep = ";",
    header = FALSE,
    col.names = c("Date","Deaths")
  )

data$Date = as.Date(data$Date)
data$Date = format(data$Date, "%d/%m/%Y")


data_table = formattable(
                data,
                align = c("l","r"),
                list("Date" = formatter("span", style = ~ style(color = "grey",font.weight = "bold")))
              )
  
print(data_table)


data <-
  read.csv(
    "./data/cases_by_Sex.csv",
    sep = ",",
    header = FALSE,
    col.names = c("Sex","Infections")
  )


data_table = formattable(
  data,
  align = c("l","r"),
  list("Sex" = formatter("span", style = ~ style(color = "grey",font.weight = "bold")))
)

print(data_table)


