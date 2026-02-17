import configparser
import os
import sys

class Config:
    pass
config = Config()

def load_config():
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    config_parser = configparser.ConfigParser()
    config_path = os.path.join(project_root, 'config.ini')
    
    if not os.path.exists(config_path):
        print(f"❌ FATAL ERROR: config.ini not found at path: {config_path}"); sys.exit(1)
            
    config_parser.read(config_path)

    try:
        # Paths
        config.scrapper_output_dir = os.path.join(project_root, config_parser.get('Paths', 'ScrapperOutputDir'))
        config.log_dir = os.path.join(project_root, config_parser.get('Paths', 'LogDir'))
        config.output_dir = os.path.join(project_root, config_parser.get('Paths', 'OutputDir'))
        # ImageCacheDir is deprecated - images are now fetched directly from URLs without disk caching
        config.image_cache_dir = os.path.join(project_root, config_parser.get('Paths', 'ImageCacheDir', fallback='image_cache'))
        config.key_report_dir = os.path.join(project_root, config_parser.get('Paths', 'KeyReportDir'))
        config.mosaic_images_dir = os.path.join(project_root, config_parser.get('Paths', 'MosaicImagesDir'))
        config.category_json = os.path.join(project_root, config_parser.get('Paths', 'CategoryJson'))
        config.rules_json = os.path.join(project_root, config_parser.get('Paths', 'RulesJson'))
        config.project_root = project_root

        # Settings - Mistral AI model
        config.mistral_model = config_parser.get('Settings', 'MistralModel', fallback='mistral-medium-2508')
        # Keep gemini_model* attributes pointing to the Mistral model so the rest of the codebase
        # (which references config.gemini_model_*) works without changes
        config.gemini_model = config.mistral_model
        config.gemini_model_promo_check = config.mistral_model
        config.gemini_model_classification = config.mistral_model
        config.gemini_model_dually_verification = config.mistral_model
        config.max_images = config_parser.getint('Settings', 'MaxImagesPerAd')
        config.ai_checkpoint_interval = config_parser.getint('Settings', 'AiCheckpointInterval', fallback=5)
        config.scraper_checkpoint_interval = config_parser.getint('Settings', 'ScraperCheckpointInterval', fallback=50)
        config.include_example_images = config_parser.getboolean('Settings', 'IncludeExampleImagesInPrompt')
        config.high_confidence_threshold = config_parser.getfloat('Settings', 'HighConfidenceThreshold', fallback=95.0)
        config.scraper_sanity_check = config_parser.getint('Settings', 'ScraperSanityCheck', fallback=50)
        config.api_key_daily_limit = config_parser.getint('Settings', 'ApiKeyDailyLimit', fallback=250)
        config.rate_limit_rpm = config_parser.getint('Settings', 'RateLimitRPM', fallback=60)
        
        # Dually Detection Settings
        config.enable_darth_cv2_dually = config_parser.getboolean('Settings', 'EnableDarthCV2Dually', fallback=True)
        config.enable_dually_llm_verification = config_parser.getboolean('Settings', 'EnableDuallyLLMVerification', fallback=True)
        config.darth_cv2_dually_threshold = config_parser.getint('Settings', 'DarthCV2DuallyThreshold', fallback=2)
        
        # Mosaic Image Saving
        config.save_mosaic_images = config_parser.getboolean('Settings', 'SaveMosaicImages', fallback=False)

        # DB API Credentials
        config.db_api_client_id = config_parser.get('DB_API', 'ClientId', fallback='')
        config.db_api_client_secret = config_parser.get('DB_API', 'ClientSecret', fallback='')
        config.db_api_grant_type = config_parser.get('DB_API', 'GrantType', fallback='client_credentials')

        # Mistral AI API Keys - reads from [Mistral_Keys] section
        config.gemini_api_keys_info = []
        for i, (_, key) in enumerate(config_parser.items('Mistral_Keys')):
            config.gemini_api_keys_info.append({
                "key": key,
                "original_index": i + 1,
                "partial_key": f"{key[:6]}...{key[-4:]}"
            })
        
        # Also keep a simple list of key strings for backward compatibility
        config.gemini_api_keys = [info['key'] for info in config.gemini_api_keys_info]

        if not config.gemini_api_keys:
            raise ValueError("No Mistral API keys found in [Mistral_Keys] section of config.ini.")

    except (configparser.NoSectionError, configparser.NoOptionError, ValueError) as e:
        print(f"❌ CONFIGURATION ERROR in config.ini: {e}"); sys.exit(1)