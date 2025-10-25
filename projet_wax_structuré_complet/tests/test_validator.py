from wax_pipeline.validator import validate_column_names

def test_validate_column_names_pass():
    df_cols = ["id", "name"]
    config_cols = ["id", "name"]
    assert validate_column_names(df_cols, config_cols) is None
