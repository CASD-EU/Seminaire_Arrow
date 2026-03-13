library(arrow)
library(dplyr)

emplacement_dataset <- "C:\\Users\\afeldmann\\Desktop\\openamir"

open_dataset(emplacement_dataset) %>%
  group_by(annee, mois) %>%
  summarise(FLT_PAI_MNT = sum(FLT_PAI_MNT)) %>%
  collect()
