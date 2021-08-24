# In pyspark we do the ML different. We do not do any test or train split
# Whatever features we will have will be grouped togather to form an independent feature 
# We use VectorAssembler for that purpose
from pyspark.ml.feature import VectorAssembler
featureassembler = VectorAssembler(inputCols = [""], outputCols = [""])
output.featuressembler.transform(training)

from pyspark.ml.regression import LinearRegression
##train test split
train_data,test_data=finalized_data.randomSplit([0.75,0.25])
regressor=LinearRegression(featuresCol='Independent Features', labelCol='Salary')
regressor=regressor.fit(train_data)

regressor.coefficients
regressor.intercept
pred_results=regressor.evaluate(test_data)
pred_results.predictions.show()
pred_results.meanAbsoluteError,pred_results.meanSquaredError
