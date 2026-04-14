# @Author: Robin Steuteville
# Objective: DC Venue and Day Type Kaplan-Meier Survival Analysis
# Date: April 13, 2026
# Contact: Robin.Steuteville@nlr.gov
# Corresponding Author: Ranjit Desai (Ranjit.Desai@nlr.gov)
# This script runs a Kaplan-Meier survival analysis on the sample data provided, and
# returns the resulting survival curves as plots. This analysis, with more data,
# was used in the paper. However, note that this analysis doesn't include
# the confidence intervals contained in the paper, since those used clustered
# standard errors, which required confidential station ids.
# Note: this file is set up to access the data located in the "data" subdirectory
# and save the resulting plots to a "day_type_and_time_survival_analysis_plots"
# subdirectory, which it will create if the directory does not exist.

require("rstudioapi")
current_path <- getActiveDocumentContext()$path
setwd(dirname(current_path))

require("formatR")
require("tidyr")
require("ggplot2")
library(survival)
library(survminer)
library(cowplot)

################################################################################################
################################################################################################
# Defining and reading in variables
################################################################################################
################################################################################################

################################################
# basic variables to define
################################################

# creating output directory
outdir <- paste0(getwd(), "/day_type_and_time_survival_analysis_plots/")

# creating output directory if it does not exist
if (dir.exists(outdir)) {} else {
  dir.create(file.path(outdir))
}

# defining strings for day types
day_types <- c("Weekday", "Weekend")

################################################
# data frames
################################################

# reading in DC dataframe
DC_df <- read.csv(paste0(getwd(), "/data/representative_DC_df.csv"))

DC_df <- as.data.frame(DC_df)

# Updating names of six hour blocks and factoring them to ensure proper ordering of the legend
DC_df$six_hour_blocks <- replace(DC_df$six_hour_blocks, DC_df$six_hour_blocks == "12am - 5:59am", "Early morning, 12 - 6am")
DC_df$six_hour_blocks <- replace(DC_df$six_hour_blocks, DC_df$six_hour_blocks == "12pm - 5:59pm", "Afternoon, 12 - 6pm")
DC_df$six_hour_blocks <- replace(DC_df$six_hour_blocks, DC_df$six_hour_blocks == "6am - 11:59am", "Morning, 6am - 12pm")
DC_df$six_hour_blocks <- replace(DC_df$six_hour_blocks, DC_df$six_hour_blocks == "6pm - 11:59pm", "Evening, 6pm - 12am")
DC_df$six_hour_blocks <- factor(DC_df$six_hour_blocks, levels = c("Early morning, 12 - 6am", "Morning, 6am - 12pm", "Afternoon, 12 - 6pm", "Evening, 6pm - 12am"))

# creating column of all 1's for purpose of showing none of points are censored
# https://www.rdocumentation.org/packages/survival/versions/2.11-4/topics/Surv
DC_df$censored <- 1

# converting connected mins to connected hours
DC_df$connected_hours <- DC_df$connected_mins / 60.

# Creating a Surv object to do survival analysis on the DC dataframe
surv_all_DC <- Surv(time = DC_df$connected_hours, event = DC_df$censored)

# Subsetting DC dataframe for day type analysis and creating Surv object
DC_day_type_df <- subset(DC_df, day_type %in% day_types)
DC_day_type_df$day_type <- factor(DC_day_type_df$day_type, levels = day_types)
surv_day_type_DC <- Surv(time = DC_day_type_df$connected_hours, event = DC_day_type_df$censored)

# Reading in L2 dataframe
L2_df <- read.csv(paste0(getwd(), "/data/representative_L2_df.csv"))

L2_df <- as.data.frame(L2_df)

# Updating names of six hour blocks and factoring them to ensure proper ordering of the legend
L2_df$six_hour_blocks <- replace(L2_df$six_hour_blocks, L2_df$six_hour_blocks == "12am - 5:59am", "Early morning, 12 - 6am")
L2_df$six_hour_blocks <- replace(L2_df$six_hour_blocks, L2_df$six_hour_blocks == "12pm - 5:59pm", "Afternoon, 12 - 6pm")
L2_df$six_hour_blocks <- replace(L2_df$six_hour_blocks, L2_df$six_hour_blocks == "6am - 11:59am", "Morning, 6am - 12pm")
L2_df$six_hour_blocks <- replace(L2_df$six_hour_blocks, L2_df$six_hour_blocks == "6pm - 11:59pm", "Evening, 6pm - 12am")
L2_df$six_hour_blocks <- factor(L2_df$six_hour_blocks, levels = c("Early morning, 12 - 6am", "Morning, 6am - 12pm", "Afternoon, 12 - 6pm", "Evening, 6pm - 12am"))

# creating column of all 1's for purpose of showing none of points are censored
# https://www.rdocumentation.org/packages/survival/versions/2.11-4/topics/Surv
L2_df$censored <- 1

# converting connected mins to connected hours
L2_df$connected_hours <- L2_df$connected_mins / 60.

# Creating a Surv object to do survival analysis on the DC dataframe
surv_all_L2 <- Surv(time = L2_df$connected_hours, event = L2_df$censored)

# Subsetting L2 dataframe for day type analysis and creating Surv object
L2_day_type_df <- subset(L2_df, day_type %in% day_types)
L2_day_type_df$day_type <- factor(L2_day_type_df$day_type, levels = day_types)
surv_day_type_L2 <- Surv(time = L2_day_type_df$connected_hours, event = L2_day_type_df$censored)

# Defining plot colors

time_colors <- c("Early morning, 12 - 6am" = "darkolivegreen3", "Morning, 6am - 12pm" = "lightgoldenrod", "Afternoon, 12 - 6pm" = "salmon", "Evening, 6pm - 12am" = "skyblue")
time_colors_updated <- c("darkolivegreen3", "lightgoldenrod", "salmon", "skyblue")

day_type_colors <- c("Weekday" = "salmon", "Weekend" = "skyblue")

day_type_colors_updated <- c("salmon", "skyblue")

title_size <- 14.8
smaller_fontsize <- 13.5
pval_fontsize <- 4.2

################################################################################################
################################################################################################
# Plots
################################################################################################
################################################################################################

L2_plot_theme <- function(base_size) {
  theme_classic(base_size = base_size, base_family = "Helvetica") %+replace%
    theme(
      axis.text = element_text(size = rel(0.9)),
      plot.title = element_text(size = title_size, hjust = 0.5, vjust = 3, face = "bold", color = "Black"),
      axis.ticks.x.bottom = element_line(colour = "Black"),
      axis.ticks.y.left = element_line(colour = "Black"),
      axis.text.x = element_text(size = smaller_fontsize, color = "Black"),
      axis.text.y = element_text(size = smaller_fontsize),
      axis.title.x = element_text(size = smaller_fontsize, hjust = 0.5, color = "Black"),
      axis.title.y = element_text(size = smaller_fontsize, angle = 90, hjust = 0.5, vjust = 3),
      legend.text = element_text(size = smaller_fontsize),
      legend.key = element_blank(),
      legend.title = element_blank(),
      panel.background = element_blank(),
      legend.background = element_rect(fill = "white", colour = "black", size = 0.5),
      panel.border = element_blank(),
      panel.grid.major = element_line(colour = "grey70", size = 0.3),
      panel.grid.minor = element_line(colour = "grey70", size = 0.6),
      strip.background = element_rect(fill = "grey70", colour = "grey70", size = 0.2),
      axis.line.x = element_line(color = "Black", size = 1.06),
      axis.line.y = element_line(color = "Black", size = 1.06),
      plot.margin = unit(c(14.0, 6.0, 6.0, 6.0), "pt")
    )
}

DC_plot_theme <- function(base_size) {
  theme_classic(base_size = base_size, base_family = "Helvetica") %+replace%
    theme(
      axis.text = element_text(size = rel(0.8)),
      plot.title = element_text(size = title_size, hjust = 0.5, vjust = 3, face = "bold", color = "Black"),
      axis.ticks.x.bottom = element_line(colour = "Black"),
      axis.ticks.y.left = element_line(colour = "Black"),
      axis.text.x = element_text(size = smaller_fontsize, color = "Black"),
      axis.text.y = element_text(size = smaller_fontsize),
      axis.title.x = element_text(size = smaller_fontsize, hjust = 0.5, color = "Black"),
      axis.title.y = element_text(size = smaller_fontsize, angle = 90, hjust = 0.5, vjust = 3),
      legend.text = element_text(size = smaller_fontsize, color = "Black"),
      legend.key = element_blank(),
      legend.title = element_blank(),
      panel.background = element_blank(),
      legend.background = element_rect(fill = "white", colour = "black", size = 0.5),
      panel.border = element_blank(),
      panel.grid.major = element_line(colour = "grey70", size = 0.3),
      panel.grid.minor = element_line(colour = "grey70", size = 0.6),
      strip.background = element_rect(fill = "grey70", colour = "grey70", size = 0.2),
      axis.line.x = element_line(color = "Black", size = 1.06),
      axis.line.y = element_line(color = "Black", size = 1.06),
      plot.margin = unit(c(14.0, 6.0, 6.0, 6.0), "pt")
    )
}

################################################
# survival analysis by time of day
################################################

# DC
fit_DC <- survfit(surv_all_DC ~ six_hour_blocks, data = DC_df)
names(fit_DC$strata) <- gsub("six_hour_blocks=", "", names(fit_DC$strata))
surv_diff_DC_time <- survdiff(surv_all_DC ~ six_hour_blocks, data = DC_df)
pval_DC_time <- pchisq(surv_diff_DC_time$chisq, length(surv_diff_DC_time$n) - 1, lower.tail = FALSE)
pval_text_DC_time <- ifelse(pval_DC_time < 0.001, "p < 0.001", paste("p =", signif(pval_DC_time, 2)))
surv_plot_DC_time <- ggsurvplot(fit_DC,
  xlab = "DC Connected Time (hours)",
  legend = c(0.75, 0.85),
  break.x.by = 0.25,
  palette = time_colors_updated,
  xlim = c(0., 1.5),
  size = 1.8,
  ggtheme = DC_plot_theme(base_size = 10.2)
)
surv_plot_DC_time$plot <- surv_plot_DC_time$plot +
  annotate("rect", xmin = 0.02, xmax = 0.35, ymin = 0.02, ymax = 0.12, fill = "white", color = "black", size = 0.5) +
  annotate("text", x = 0.185, y = 0.07, label = pval_text_DC_time, size = pval_fontsize, hjust = 0.5)

# L2
fit_L2 <- survfit(surv_all_L2 ~ six_hour_blocks, data = L2_df)
names(fit_L2$strata) <- gsub("six_hour_blocks=", "", names(fit_L2$strata))
surv_diff_L2_time <- survdiff(surv_all_L2 ~ six_hour_blocks, data = L2_df)
pval_L2_time <- pchisq(surv_diff_L2_time$chisq, length(surv_diff_L2_time$n) - 1, lower.tail = FALSE)
pval_text_L2_time <- ifelse(pval_L2_time < 0.001, "p < 0.001", paste("p =", signif(pval_L2_time, 2)))
surv_plot_L2_time <- ggsurvplot(fit_L2,
  xlab = "L2 Connected Time (hours)",
  legend = c(0.75, 0.85),
  break.x.by = 1.,
  xlim = c(0., 12.),
  size = 1.8,
  palette = time_colors_updated,
  ggtheme = L2_plot_theme(base_size = 10.2)
)
surv_plot_L2_time$plot <- surv_plot_L2_time$plot +
  annotate("rect", xmin = 0.2, xmax = 2.8, ymin = 0.02, ymax = 0.12, fill = "white", color = "black", size = 0.5) +
  annotate("text", x = 1.5, y = 0.07, label = pval_text_L2_time, size = pval_fontsize, hjust = 0.5)

################################################
# survival analysis by day type
################################################

# L2
fit_L2_day_type <- survfit(surv_day_type_L2 ~ day_type, data = L2_day_type_df)
names(fit_L2_day_type$strata) <- gsub("day_type=", "", names(fit_L2_day_type$strata))
surv_diff_L2_day_type <- survdiff(surv_day_type_L2 ~ day_type, data = L2_day_type_df)
pval_L2_day_type <- pchisq(surv_diff_L2_day_type$chisq, length(surv_diff_L2_day_type$n) - 1, lower.tail = FALSE)
pval_text_L2_day_type <- ifelse(pval_L2_day_type < 0.001, "p < 0.001", paste("p =", signif(pval_L2_day_type, 2)))
surv_plot_L2_day_type <- ggsurvplot(fit_L2_day_type,
  title = "Level 2",
  xlab = "L2 Connected Time (hours)",
  legend = c(0.75, 0.8),
  break.x.by = 1.,
  xlim = c(0., 12.),
  size = 1.8,
  palette = day_type_colors_updated,
  ggtheme = L2_plot_theme(base_size = 10.2)
)
surv_plot_L2_day_type$plot <- surv_plot_L2_day_type$plot +
  annotate("rect", xmin = 0.2, xmax = 2.8, ymin = 0.02, ymax = 0.12, fill = "white", color = "black", size = 0.5) +
  annotate("text", x = 1.5, y = 0.07, label = pval_text_L2_day_type, size = pval_fontsize, hjust = 0.5)

# DC
fit_DC_day_type <- survfit(surv_day_type_DC ~ day_type, data = DC_day_type_df)
names(fit_DC_day_type$strata) <- gsub("day_type=", "", names(fit_DC_day_type$strata))
surv_diff_DC_day_type <- survdiff(surv_day_type_DC ~ day_type, data = DC_day_type_df)
pval_DC_day_type <- pchisq(surv_diff_DC_day_type$chisq, length(surv_diff_DC_day_type$n) - 1, lower.tail = FALSE)
pval_text_DC_day_type <- ifelse(pval_DC_day_type < 0.001, "p < 0.001", paste("p =", signif(pval_DC_day_type, 2)))
surv_plot_DC_day_type <- ggsurvplot(fit_DC_day_type,
  title = "DC",
  xlab = "DC Connected Time (hours)",
  legend = c(0.75, 0.8),
  break.x.by = 0.25,
  xlim = c(0., 1.5),
  size = 1.8,
  palette = day_type_colors_updated,
  ggtheme = DC_plot_theme(base_size = 10.2)
)
surv_plot_DC_day_type$plot <- surv_plot_DC_day_type$plot +
  annotate("rect", xmin = 0.02, xmax = 0.35, ymin = 0.02, ymax = 0.12, fill = "white", color = "black", size = 0.5) +
  annotate("text", x = 0.185, y = 0.07, label = pval_text_DC_day_type, size = pval_fontsize, hjust = 0.5)

combined_plot <- plot_grid(
  surv_plot_L2_day_type$plot,
  surv_plot_DC_day_type$plot,
  surv_plot_L2_time$plot,
  surv_plot_DC_time$plot,
  ncol = 2,
  nrow = 2,
  align = "hv",
  axis = "tblr"
)

ggsave(plot = combined_plot, filename = paste0("survival_curves_day_type_and_time_sample_data_", format(Sys.Date(), "%m_%d_%y"), ".jpeg"), path = outdir, width = 6912, height = 4600, dpi = 600, units = "px", device = "jpeg", limitsize = FALSE)
