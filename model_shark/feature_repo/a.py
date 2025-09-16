from feast import FeatureStore
store = FeatureStore(repo_path="model_shark/feature_repo")
print(store.list_feature_views())