# CorrPlot without labels
ggcorrplot(corr_matrix, hc.order = TRUE, type = "lower", lab = FALSE, ) + theme(axis.text.x = element_blank(),
    axis.text.y = element_blank())


ggpairs(
    df.small,
    
    lower = list(
        continuous = wrap("points", size = 0.4, alpha = 0.3)
    ),
    
    diag = "blankDiag",
    
    upper = list(
        continuous = wrap("blank")  # empty upper triangle
    ),
    
    axisLabels = "none",
    
    switch = "both",
    columnLabels = tempNames
    
) +
    theme(
        strip.text = element_text(size = 6),
        axis.text  = element_text(size = 5)
    )
