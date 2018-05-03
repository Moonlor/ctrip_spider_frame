print(__doc__)

import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import math

from sklearn import ensemble
from sklearn import datasets
from sklearn.utils import shuffle
from sklearn.metrics import mean_squared_error
from sklearn.metrics import classification_report
from sklearn.model_selection import GridSearchCV

# #############################################################################
# Load data

# training data set
csv_data = pd.read_csv('./events1617.csv')
np_data = np.array(csv_data.values)

# test data set
csv_data1 = pd.read_csv('./1809_use_old_events_test_set.csv')
np_data1 = np.array(csv_data1.values)

# prediction data set
csv_data2 = pd.read_csv('./1809_use_old_events_pre_set.csv')
np_data2 = np.array(csv_data2.values)

pre_names = np.empty((1, csv_data1.shape[1]))

x_pre = np_data1[:, :-1]
y_pre = np_data1[:, -1]

x_real_pre = np_data2[:, :-1]
y_real_pre = np_data2[:, -1]

x = np_data[:, :-1]
y = np_data[:, -1]

name_list = []

# fetch the names of each feature
for name in csv_data:
    name_list.append(name)

x_names = np.array(name_list)[:-1]
y_name = np.array(name_list)[-1]

# all training data is used to train the model
offset = int(x.shape[0] * 1)
X_train, y_train = x[:offset], y[:offset]
X_test, y_test = x_pre, y_pre

# #############################################################################
# Fit regression model

# params searching grid
param_grid = {'n_estimators': [500, 1000, 2000, 4000],
              'max_depth': [2, 3, 5, 8, 12], 'min_samples_split': [2, 4, 6, 8],
          'learning_rate': [0.01, 0.05]}
# best params
params = {'n_estimators': 4000, 'max_depth': 2, 'min_samples_split': 6,
          'learning_rate': 0.01, 'loss': 'ls'}

# search for the best params
# clf = GridSearchCV(ensemble.GradientBoostingRegressor(loss='ls'), param_grid)
clf = ensemble.GradientBoostingRegressor(**params)

# training model
clf.fit(X_train, y_train)
mse = mean_squared_error(y_test, clf.predict(X_test))
print("MSE: %.4f" % mse)

# test prediction result
y_pre = clf.predict(X_test)

# the 9th round prediction result
y_real_pre = clf.predict(x_real_pre)

print("The result of the 9th round:")
print(y_real_pre)
print("=============================")

dis = 0.1

for i in range(len(y_pre)):
    if y_pre[i] > dis :
        y_pre[i] = 1.0
    elif y_pre[i] < -dis:
        y_pre[i] = -1.0
    else:
        y_pre[i] = 0.0

result = 0
for i in range(len(y_pre)):
    if math.fabs(y_pre[i] - y_test[i]) < 0.0001:
        result += 1

print(result / 64)

print(classification_report(y_test, y_pre))

# #############################################################################
# Plot training deviance

# compute test set deviance
test_score = np.zeros((params['n_estimators'],), dtype=np.float64)

for i, y_pred in enumerate(clf.staged_predict(X_test)):
    test_score[i] = clf.loss_(y_test, y_pred)

plt.figure(figsize=(12, 6))
plt.subplot(1, 2, 1)
plt.title('Deviance')
plt.plot(np.arange(params['n_estimators']) + 1, clf.train_score_, 'b-',
         label='Training Set Deviance')
plt.plot(np.arange(params['n_estimators']) + 1, test_score, 'r-',
         label='Test Set Deviance')
plt.legend(loc='upper right')
plt.xlabel('Boosting Iterations')
plt.ylabel('Deviance')

# #############################################################################
# Plot feature importance

feature_importance = clf.feature_importances_
# make importances relative to max importance
feature_importance = 100.0 * (feature_importance / feature_importance.max())
sorted_idx = np.argsort(feature_importance)
pos = np.arange(sorted_idx.shape[0]) + .5
plt.subplot(1, 2, 2)
plt.barh(pos, feature_importance[sorted_idx], align='center')
plt.yticks(pos, x_names[sorted_idx])
plt.xlabel('Relative Importance')
plt.title('Variable Importance')
plt.show()
