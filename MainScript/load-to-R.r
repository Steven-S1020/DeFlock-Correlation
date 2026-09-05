# Load Libraries
library(dplyr)
library(leaps)

# Load Raw Data
df.raw <- read.csv("./data-main.csv")

# df.raw$participation_287g_level <- factor(df.raw$task_force_model +
# df.raw$warrant_service_officer + df.raw$jail_enforcement_model, levels = 0:3,
# ordered = TRUE) df.raw <- df.raw %>% select(-task_force_model,
# -warrant_service_officer, -jail_enforcement_model)

# Remove redundant columns that sum to a total Ex. pop_male + pop_female =
# pop_total, so pop_female is removed
redundant_cols <- c("pop_female", "pop_with_health_insurance", "pop_in_labor_force",
    "civilian_labor_force", "pop_citizen_over_18", "pop_native", "pop_citizen_over_18_female",
    "naturalized_citizen", "pop_noninstitutionalized", "pop_total", "median_household_income_dollars",
    "pop_in_school", "civilian_labor_force_employed", "worker_class_government",
    "avg_household_size", "attainment_gt_12th", "pop_male")

# Keep state_fips for regional filtering, remove negative values
df.states <- df.raw %>%
    filter(!if_any(everything(), ~. < 0)) %>%
    dplyr::select(-all_of(redundant_cols))

# State FIPS codes by region
fips.ne <- c(9, 23, 25, 33, 34, 36, 42, 44, 50)
fips.mw <- c(17, 18, 19, 20, 26, 27, 29, 31, 38, 39, 46, 55)
fips.s <- c(1, 5, 10, 11, 12, 13, 21, 22, 24, 28, 37, 40, 45, 47, 48, 51, 54)
fips.w <- c(2, 4, 6, 8, 15, 16, 30, 32, 35, 41, 49, 53, 56)

# Create regional dataframes and remove zeros
df.ne <- df.states %>%
    filter(state_fips %in% fips.ne, alpr_total != 0) %>%
    dplyr::select(-(1:6))
df.mw <- df.states %>%
    filter(state_fips %in% fips.mw, alpr_total != 0)  # %>%
# dplyr::select(-(1:6))
df.s <- df.states %>%
    filter(state_fips %in% fips.s, alpr_total != 0) %>%
    dplyr::select(-(1:6))
df.w <- df.states %>%
    filter(state_fips %in% fips.w, alpr_total != 0) %>%
    dplyr::select(-(1:6))

# Remove state_fips and unnecessary columns for modeling
df.clean <- df.raw %>%
    filter(!if_any(everything(), ~. < 0)) %>%
    dplyr::select(-all_of(redundant_cols))

# National dataframe with zeros removed
df <- df.clean %>%
    filter(alpr_total != 0)

df.small.us <- df %>%
    select(alpr_total, pop_in_households, pop_in_college, attainment_lt_8th, attainment_gt_bachelors,
        pop_foreign_born, avg_household_income_dollars, pop_not_in_labor_force, work_commute_public_transport,
        work_commute_other, pop_without_health_insurance, pop_hispanic_alone, pop_white_alone,
        pop_citizen_over_18_male, pct_republican_vote, task_force_model)

df.small.mw <- df.mw %>%
    select(alpr_total, pop_in_households, foreign_born_europe, foreign_born_asia,
        foreign_born_latin_america, households_with_computer, households_with_internet,
        avg_household_income_dollars, pop_not_in_labor_force, work_commute_public_transport,
        worker_class_private_or_salary, pop_hispanic_alone, pop_white_alone, pop_black_alone,
        pop_two_or_more_races, pop_citizen_over_18_male)

names.small.us <- c("ALPR Total", "Households", "College", "< 8th Grade", "> Bachelors",
    "Foreign Born", "Avg Income", "Not in Labor Force", "Public Transport", "Other Commute",
    "No Health Insurance", "Hispanic", "White", "Male Citizens 18+", "Republican Vote",
    "Task Force")

names.small.mw <- c("ALPR Total", "Households", "European Born", "Asian Born", "Latin Born",
    "Computer", "Internet", "Avg Income", "Not in Labor Force", "Public Transport",
    "Private/Salary", "Hispanic", "White", "Black", "Two+ Races", "Male Citizens 18+")

vars.us <- c("alpr_total", "pop_in_households", "pop_in_college", "attainment_lt_8th",
    "attainment_gt_bachelors", "pop_foreign_born", "avg_household_income_dollars",
    "pop_not_in_labor_force", "work_commute_public_transport", "work_commute_other",
    "pop_without_health_insurance", "pop_hispanic_alone", "pop_white_alone", "pop_citizen_over_18_male",
    "pct_republican_vote", "task_force_model")

vars.mw <- c("alpr_total", "pop_in_households", "work_commute_public_transport",
    "pop_hispanic_alone", "pop_white_alone", "pop_black_alone", "pop_two_or_more_races")
