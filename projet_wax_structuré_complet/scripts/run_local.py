from wax_pipeline.config import Config
from wax_pipeline.file_processor import extract_zip_file, load_excel_config
from wax_pipeline.ingestion import launch_ingestion

def main():
    config = Config(use_widgets=False)
    extract_zip_file(config.zip_path, config.extract_dir)
    df_configs = load_excel_config(config.excel_path)
    launch_ingestion(df_configs, config)

if __name__ == "__main__":
    main()
