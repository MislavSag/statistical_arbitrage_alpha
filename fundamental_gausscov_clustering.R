library(data.table)


# SET UP ------------------------------------------------------------------
# set calendar
qlcal::setCalendar("UnitedStates/NYSE")

# Paths
PREDICTORS_SAVE = "D:/strategies/statsarb"


# DATA --------------------------------------------------------------------
# Import predictors
predictors = fread(file.path(PREDICTORS_SAVE, "predctors.csv"))
format(object.size(predictors), unit = "auto")


specFfm()
