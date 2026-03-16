library(arrow)
library(dplyr)

emplacement_dataset <- "C:\\Users\\afeldmann\\Desktop\\openamir"

open_dataset(emplacement_dataset) %>%
  group_by(annee, mois) %>%
  summarise(FLT_PAI_MNT = sum(FLT_PAI_MNT)) %>%
  show_query()

open_dataset(emplacement_dataset) %>%
  group_by(annee, mois) %>%
  summarise(FLT_PAI_MNT = sum(FLT_PAI_MNT)) %>%
  collect()



library(readr)
schema_readr <- cols_only(
  FLX_ANN_MOI = col_character(),
  ORG_CLE_REG = col_character(),
  AGE_BEN_SNDS = col_character(),
  BEN_RES_REG = col_character(),
  BEN_CMU_TOP = col_character(),
  BEN_QLT_COD = col_character(),
  BEN_SEX_COD = col_character(),
  DDP_SPE_COD = col_character(),
  ETE_CAT_SNDS = col_character(),
  ETE_REG_COD = col_character(),
  ETE_TYP_SNDS = col_character(),
  ETP_REG_COD = col_character(),
  ETP_CAT_SNDS = col_character(),
  MDT_TYP_COD = col_character(),
  MFT_COD = col_character(),
  PRS_FJH_TYP = col_character(),
  PRS_ACT_COG = col_double(),
  PRS_ACT_NBR = col_double(),
  PRS_ACT_QTE = col_double(),
  PRS_DEP_MNT = col_double(),
  PRS_PAI_MNT = col_double(),
  PRS_REM_BSE = col_double(),
  PRS_REM_MNT = col_double(),
  FLT_ACT_COG = col_double(),
  FLT_ACT_NBR = col_double(),
  FLT_ACT_QTE = col_double(),
  FLT_PAI_MNT = col_double(),
  FLT_DEP_MNT = col_double(),
  FLT_REM_MNT = col_double(),
  SOI_ANN = col_integer(),
  SOI_MOI = col_integer(),
  ASU_NAT = col_character(),
  ATT_NAT = col_character(),
  CPL_COD = col_character(),
  CPT_ENV_TYP = col_character(),
  DRG_AFF_NAT = col_character(),
  ETE_IND_TAA = col_character(),
  EXO_MTF = col_character(),
  MTM_NAT = col_character(),
  PRS_NAT = col_character(),
  PRS_PPU_SEC = col_character(),
  PRS_REM_TAU = col_double(),
  PRS_REM_TYP = col_character(),
  PRS_PDS_QCP = col_character(),
  EXE_INS_REG = col_character(),
  PSE_ACT_SNDS = col_character(),
  PSE_ACT_CAT = col_character(),
  PSE_SPE_SNDS = col_character(),
  PSE_STJ_SNDS = col_character(),
  PRE_INS_REG = col_character(),
  PSP_ACT_SNDS = col_character(),
  PSP_ACT_CAT = col_character(),
  PSP_SPE_SNDS = col_character(),
  PSP_STJ_SNDS = col_character(),
  TOP_PS5_TRG = col_character()
)

lapply(list.files(input_csv),
       function(nom_csv) {
         annee <- substr(nom_csv, 2L, 5L)
         mois <- substr(nom_csv, 6L, 7L)
         dest_parquet <- file.path(input_parquet, paste0("annee=", annee), paste0("mois=", mois))
         dir.create(dest_parquet, recursive = TRUE)
         write_chunk_to_parquet <- function(x, pos)  {
           write_parquet(x, file.path(dest_parquet, paste0(sprintf("%020d", pos), ".parquet")))
         }
         read_csv2_chunked(file = file.path(input_csv, nom_csv),
                           callback = SideEffectChunkCallback$new(write_chunk_to_parquet),
                           chunk_size = 1000000L,
                           col_types = schema_readr)
       }
)