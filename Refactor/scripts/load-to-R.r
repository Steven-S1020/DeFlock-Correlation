# Load Libraries
library(dplyr)
library(leaps)

# Load Raw Data
df.raw <- read.csv("./final_data/merged.csv")

# Keep state_fips for regional filtering, remove negative values
df.states <- df.raw %>%
    filter(!if_any(everything(), ~. < 0))

# State FIPS codes by region
fips.ne <- c(9, 23, 25, 33, 34, 36, 42, 44, 50)
fips.mw <- c(17, 18, 19, 20, 26, 27, 29, 31, 38, 39, 46, 55)
fips.s <- c(1, 5, 10, 11, 12, 13, 21, 22, 24, 28, 37, 40, 45, 47, 48, 51, 54)
fips.w <- c(2, 4, 6, 8, 15, 16, 30, 32, 35, 41, 49, 53, 56)

# Create regional dataframes and remove zeros
df.ne <- df.states %>%
    filter(state %in% fips.ne, alpr_total != 0) %>%
    dplyr::select(-(1:4))
df.mw <- df.states %>%
    filter(state %in% fips.mw, alpr_total != 0) %>%
    dplyr::select(-(1:4))
df.s <- df.states %>%
    filter(state %in% fips.s, alpr_total != 0) %>%
    dplyr::select(-(1:4))
df.w <- df.states %>%
    filter(state %in% fips.w, alpr_total != 0) %>%
    dplyr::select(-(1:4))

# Remove state_fips and unnecessary columns for modeling
df.clean <- df.raw %>%
    filter(!if_any(everything(), ~. < 0))

# National dataframe with zeros removed
df <- df.clean %>%
    filter(alpr_total != 0) %>%
    dplyr::select(-(1:4))

vars.all <- colnames(df)[c(1, 27, 24, 41, 31, 25, 38, 39, 28, 30, 22, 35, 15, 37,
    33, 13, 16, 14, 18, 12, 7, 34, 36, 44, 43, 45, 42, 40, 31, 26)]

vars.mw <- c("alpr_total", "pop_NH_AIAN", "pop_NH_some_other", "pop_NH_asian", "pop_unhoused",
    "pop_households_fs_snap", "pct_NH_asian", "pct_NH_some_other")

vars.s <- c("alpr_total", "pct_households_fs_snap", "pop_NH_NHPI", "pop_NH_some_other",
    "pop_NH_asian", "pop_NH_black", "pop_households_fs_snap", "pct_NH_asian", "pct_private_wage_salary_workers")

