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

        # Settings
        config.timezone = config_parser.get('Settings', 'Timezone', fallback='Asia/Kolkata')
        config.gemini_model = config_parser.get('Settings', 'GeminiModel')
        # Per-prompt model configuration (falls back to GeminiModel if not set)
        config.gemini_model_promo_check = config_parser.get('Settings', 'GeminiModelPromoCheck', fallback=config.gemini_model)
        config.gemini_model_classification = config_parser.get('Settings', 'GeminiModelClassification', fallback=config.gemini_model)
        config.gemini_model_dually_verification = config_parser.get('Settings', 'GeminiModelDuallyVerification', fallback=config.gemini_model)
        config.max_images = config_parser.getint('Settings', 'MaxImagesPerAd')
        config.ai_checkpoint_interval = config_parser.getint('Settings', 'AiCheckpointInterval', fallback=5)
        config.scraper_checkpoint_interval = config_parser.getint('Settings', 'ScraperCheckpointInterval', fallback=50)
        config.include_example_images = config_parser.getboolean('Settings', 'IncludeExampleImagesInPrompt')
        config.high_confidence_threshold = config_parser.getfloat('Settings', 'HighConfidenceThreshold', fallback=95.0)
        config.scraper_sanity_check = config_parser.getint('Settings', 'ScraperSanityCheck', fallback=50)
        config.api_key_daily_limit = config_parser.getint('Settings', 'ApiKeyDailyLimit', fallback=250)
        config.rate_limit_rpm = config_parser.getint('Settings', 'RateLimitRPM', fallback=13)
        
        # Dually Detection Settings
        config.enable_darth_cv2_dually = config_parser.getboolean('Settings', 'EnableDarthCV2Dually', fallback=True)
        config.enable_dually_llm_verification = config_parser.getboolean('Settings', 'EnableDuallyLLMVerification', fallback=True)
        config.darth_cv2_dually_threshold = config_parser.getint('Settings', 'DarthCV2DuallyThreshold', fallback=2)
        
        # Mosaic Image Saving
        config.save_mosaic_images = config_parser.getboolean('Settings', 'SaveMosaicImages', fallback=False)
        config.mosaic_batch_size = config_parser.getint('Settings', 'MosaicBatchSize', fallback=3)

        # Cutaway-Cube Van Detection
        config.max_images_cutaway = config_parser.getint('Settings', 'MaxImagesForCutawayCategories', fallback=30)
        config.cutaway_categories = [
            cat.strip().lower()
            for cat in config_parser.get('Settings', 'CutawayCategories', fallback='box truck,straight truck,dry van,cargo van,stepvan').split(',')
        ]

        # Thought Summaries
        config.enable_thought_summaries = config_parser.getboolean('Settings', 'EnableThoughtSummaries', fallback=False)

        # Verbose Cache Logging
        config.verbose_cache_logging = config_parser.getboolean('Settings', 'VerboseCacheLogging', fallback=True)

        # Key Reports (session Excel reports saved to Key Reports folder)
        config.enable_key_reports = config_parser.getboolean('Settings', 'EnableKeyReports', fallback=True)

        # Audit Reports (audit Excel reports saved to Audit Reports folder)
        config.enable_audit_reports = config_parser.getboolean('Settings', 'EnableAuditReports', fallback=True)

        # AI Output files (annotated Excel files saved to AI output folder)
        config.enable_ai_output = config_parser.getboolean('Settings', 'EnableAIOutput', fallback=True)

        # Scrapper output files (Scrapper_*.xlsx skipped; DB_Fetch_*.xlsx goes to temp)
        config.enable_scrapper_output = config_parser.getboolean('Settings', 'EnableScrapperOutput', fallback=True)

        # Uploads folder (user-uploaded files go to temp instead of uploads/)
        config.enable_uploads = config_parser.getboolean('Settings', 'EnableUploads', fallback=True)

        # Log files (worker .txt log files written to logs/ folder)
        config.enable_log_files = config_parser.getboolean('Settings', 'EnableLogFiles', fallback=True)

        # CTT Platform Filter
        config.enable_ctt_platform_filter = config_parser.getboolean('Settings', 'EnableCTTPlatformFilter', fallback=False)
        config.ctt_feature_id = config_parser.get('Settings', 'CTTFeatureId', fallback='5000000080')

        # DB API Credentials
        config.db_api_token_url = config_parser.get('DB_API', 'TokenUrl', fallback='')
        config.db_api_base_url = config_parser.get('DB_API', 'BaseUrl', fallback='')
        config.db_api_trucks_url = config_parser.get('DB_API', 'TrucksUrl', fallback='')
        config.db_api_client_id = config_parser.get('DB_API', 'ClientId', fallback='')
        config.db_api_client_secret = config_parser.get('DB_API', 'ClientSecret', fallback='')
        config.db_api_grant_type = config_parser.get('DB_API', 'GrantType', fallback='client_credentials')

        # Turso DB — Ad Annotation Limit
        config.enable_ad_annotation_limit = config_parser.getboolean('Turso_DB', 'EnableAdAnnotationLimit', fallback=False)
        config.turso_db_url = config_parser.get('Turso_DB', 'TursoDbUrl', fallback='')
        config.turso_auth_token = config_parser.get('Turso_DB', 'TursoAuthToken', fallback='')
        config.max_annotation_runs = config_parser.getint('Turso_DB', 'MaxAnnotationRuns', fallback=3)

        # DB Update API Credentials (for updating ad categories in the database)
        config.db_update_token_url = config_parser.get('DB_Update_API', 'TokenUrl', fallback='')
        config.db_update_base_url = config_parser.get('DB_Update_API', 'UpdateBaseUrl', fallback='')
        config.db_update_client_id = config_parser.get('DB_Update_API', 'ClientId', fallback='')
        config.db_update_client_secret = config_parser.get('DB_Update_API', 'ClientSecret', fallback='')
        config.db_update_grant_type = config_parser.get('DB_Update_API', 'GrantType', fallback='client_credentials')

        # Grafana Loki — CDC Audit Logging
        config.enable_cdc_audit_log = config_parser.getboolean('Grafana_Loki', 'EnableCDCAuditLog', fallback=False)
        config.loki_push_url = config_parser.get('Grafana_Loki', 'LokiPushUrl', fallback='')
        config.loki_query_url = config_parser.get('Grafana_Loki', 'LokiQueryUrl', fallback='')
        config.loki_user_id = config_parser.get('Grafana_Loki', 'LokiUserId', fallback='')
        config.loki_api_key = config_parser.get('Grafana_Loki', 'LokiApiKey', fallback='')

        # Backblaze B2 Cloud Storage
        config.b2_enabled = config_parser.getboolean('Backblaze_B2', 'Enabled', fallback=False)
        config.b2_key_id = config_parser.get('Backblaze_B2', 'KeyId', fallback='')
        config.b2_application_key = config_parser.get('Backblaze_B2', 'ApplicationKey', fallback='')
        config.b2_bucket_name = config_parser.get('Backblaze_B2', 'BucketName', fallback='awacs-outputs')
        config.b2_region = config_parser.get('Backblaze_B2', 'Region', fallback='us-west-004')
        config.b2_endpoint_url = config_parser.get('Backblaze_B2', 'EndpointUrl', fallback='')
        config.b2_presigned_url_expiry = config_parser.getint('Backblaze_B2', 'PresignedUrlExpiry', fallback=3600)

        # API Keys - Now stores a list of dictionaries for rich data
        config.gemini_api_keys_info = []
        for i, (_, key) in enumerate(config_parser.items('API_Keys')):
            config.gemini_api_keys_info.append({
                "key": key,
                "original_index": i + 1,
                "partial_key": f"{key[:6]}...{key[-4:]}"
            })
        
        # Also keep a simple list of key strings for backward compatibility
        config.gemini_api_keys = [info['key'] for info in config.gemini_api_keys_info]

        if not config.gemini_api_keys:
            raise ValueError("No API keys found in config.ini.")

    except (configparser.NoSectionError, configparser.NoOptionError, ValueError) as e:
        print(f"❌ CONFIGURATION ERROR in config.ini: {e}"); sys.exit(1)