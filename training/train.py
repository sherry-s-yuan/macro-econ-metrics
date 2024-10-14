from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler

X = pd.read_csv("sample_data_features.csv")
y = pd.read_csv("sample_data_labels.csv")
feature_names = X.columns

# remove duplicate columns
corr_matrix  = np.corrcoef(X.values, rowvar=False)
threshold = 0.9  # Set the correlation threshold
to_drop = set()  # Create a set to store features to drop
n_features = corr_matrix.shape[0]
# Iterate through the correlation matrix
indices = np.where(corr_matrix > threshold)
coordinates = list(zip(indices[0], indices[1]))
for (row, col) in coordinates:
    if row == col: continue
    if row in to_drop or col in to_drop: continue
    to_drop.add(row)
# print(to_drop)
# print(len(to_drop)/len(feature_names))
# pd.DataFrame({
#     'x': indices[0],
#     'y': indices[1],
#     'std': corr_matrix[indices],
# }).to_csv('feature_corr.csv', index=False)
col_to_keep = [s for i, s in enumerate(feature_names) if i not in to_drop]
X = X[col_to_keep]

scaler = StandardScaler()

X_train, X_test, y_train, y_test = train_test_split(
    X.to_numpy(), y.to_numpy(), test_size=0.3, random_state=42
)
forest = RandomForestClassifier(random_state=0, n_estimators=10, n_jobs=-1)
forest.fit(scaler.fit_transform(X_train), y_train)

y_pred = forest.predict(scaler.transform(X_test))

accuracy = accuracy_score(y_test, y_pred)


print("accuracy")
print(accuracy)

importances = forest.feature_importances_
std = np.std([tree.feature_importances_ for tree in forest.estimators_], axis=0)

pd.DataFrame({
    'feature_name': col_to_keep,
    'importance': importances,
    'std': std,
}).to_csv('rf_feature_importance.csv', index=False)
