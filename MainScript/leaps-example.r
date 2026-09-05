library(leaps)

model <- regsubsets(alpr_total ~ .,
                    data = df,
                    nvmax = 20,
                    nbest = 1,
                    method = "backward")

plot(summary(model)$adjr2,
     xlab = "Number of Variables",
     ylab = "Adjusted R2",
     type = "b")
