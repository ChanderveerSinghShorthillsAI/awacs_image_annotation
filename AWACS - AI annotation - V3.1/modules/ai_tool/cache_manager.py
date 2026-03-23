"""
cache_manager.py — Gemini Explicit Context Caching Manager

Uses the NEW google-genai SDK (pip install google-genai) for explicit caching.
The rest of the codebase continues using the older google-generativeai SDK.

Caches the static ~5000+ token classification rules server-side so only the
dynamic content (breadcrumb + image) is sent per-ad.  Cached tokens are billed
at 90% discount ($0.03/M vs $0.30/M for Flash).
"""

import time
import base64

# === Try importing the NEW google-genai SDK for caching ===
try:
    from google import genai
    from google.genai import types
    CACHING_AVAILABLE = True
except ImportError:
    CACHING_AVAILABLE = False


class ExplicitCacheManager:
    """
    Manages Gemini explicit context caches using the google-genai SDK.
    
    - Creates one cache per (api_key, model_name) combination
    - Handles cache creation, reuse, and expiry recovery
    - Tracks cost savings from cached tokens
    - Provides verbose terminal output for monitoring
    """
    
    def __init__(self):
        # Cache store: { "cache_key" -> { "cache_name": str, "created_at": float } }
        self._cache_store = {}
        # Client store: { "api_key" -> genai.Client }
        self._clients = {}
        
        # === COST SAVINGS TRACKING ===
        self._total_cached_tokens = 0
        self._total_non_cached_tokens = 0
        self._total_savings_usd = 0.0
        self._total_listings_with_cache = 0
        self._cache_creation_count = 0
        
        # Print SDK caching availability status
        if CACHING_AVAILABLE:
            print(f"\n{'='*80}")
            print(f"✅ GEMINI EXPLICIT CACHING: google-genai SDK detected!")
            print(f"   Explicit caching is ENABLED — static classification rules will be cached server-side")
            print(f"   Cached tokens are 90% cheaper ($0.03/M vs $0.30/M for Flash)")
            print(f"{'='*80}\n")
        else:
            print(f"\n{'='*80}")
            print(f"⚠️  GEMINI EXPLICIT CACHING: google-genai SDK NOT found!")
            print(f"   Run: pip install google-genai")
            print(f"   Falling back to standard (non-cached) API calls.")
            print(f"{'='*80}\n")
    
    def _get_client(self, api_key: str):
        """Get or create a genai.Client for the given API key."""
        if api_key not in self._clients:
            self._clients[api_key] = genai.Client(api_key=api_key)
        return self._clients[api_key]
    
    def _get_cache_key(self, api_key: str, model_name: str) -> str:
        """Generate unique key for cache lookup."""
        return f"{api_key[-8:]}::{model_name}"
    
    def _get_pricing(self, model_name: str) -> tuple:
        """Returns (price_input_per_m, price_cached_per_m) based on model."""
        model = model_name.lower()
        if "3.1" in model and "lite" in model:
            return 0.25, 0.025  # gemini-3.1-flash-lite
        elif "2.5" in model and "lite" in model:
            return 0.10, 0.01  # gemini-2.5-flash-lite
        elif "lite" in model or "8b" in model:
            return 0.075, 0.03  # gemini-2.0-flash-lite
        else:
            return 0.30, 0.03  # gemini-2.5-flash (default)
    
    def _convert_old_parts_to_new(self, old_parts: list) -> list:
        """
        Convert old google-generativeai SDK parts format to new google-genai types.
        
        Old format uses: strings and {"inline_data": {"mime_type": ..., "data": base64_str}}
        New format uses: strings and types.Part.from_bytes(data=bytes, mime_type=str)
        """
        new_parts = []
        for part in old_parts:
            if isinstance(part, str):
                if part.strip():  # skip empty strings
                    new_parts.append(part)
            elif isinstance(part, dict) and 'inline_data' in part:
                # Convert old SDK inline_data dict to new SDK Part
                img_bytes = base64.b64decode(part['inline_data']['data'])
                new_parts.append(types.Part.from_bytes(
                    data=img_bytes,
                    mime_type=part['inline_data']['mime_type']
                ))
        return new_parts
    
    def generate_with_cache(self, api_key: str, model_name: str,
                            cacheable_content_parts: list,
                            context_text: str, image_bytes: bytes | list[bytes],
                            timeout: int = 45, include_thoughts: bool = False):
        """
        Creates/reuses an explicit cache for static content and generates 
        a response with cached + dynamic content.
        
        Args:
            api_key: The Gemini API key
            model_name: Model name (e.g. "gemini-2.5-flash")
            cacheable_content_parts: Static content parts (old SDK format - strings + inline_data dicts)
            context_text: The dynamic breadcrumb context text
            image_bytes: Raw image bytes for classification
            timeout: Request timeout in seconds
            
        Returns:
            (response, True) if cache was used successfully
            (None, False) if cache creation/usage failed (caller should fallback)
        """
        if not CACHING_AVAILABLE:
            return None, False
        
        try:
            client = self._get_client(api_key)
            cache_key = self._get_cache_key(api_key, model_name)
            
            # === GET OR CREATE CACHE ===
            cache_name = self._get_or_create_cache(
                client, cache_key, model_name, cacheable_content_parts
            )
            
            if cache_name is None:
                return None, False
            
            # === BUILD DYNAMIC CONTENT (breadcrumb + image(s)) ===
            dynamic_parts = [context_text]
            if isinstance(image_bytes, list):
                for img in image_bytes:
                    dynamic_parts.append(types.Part.from_bytes(data=img, mime_type='image/jpeg'))
            else:
                dynamic_parts.append(types.Part.from_bytes(data=image_bytes, mime_type='image/jpeg'))
            
            # === GENERATE WITH CACHED CONTENT ===
            gen_config = types.GenerateContentConfig(cached_content=cache_name)
            if include_thoughts:
                gen_config = types.GenerateContentConfig(
                    cached_content=cache_name,
                    thinking_config=types.ThinkingConfig(include_thoughts=True)
                )

            response = client.models.generate_content(
                model=model_name,
                contents=dynamic_parts,
                config=gen_config
            )
            
            print(f"   ✅ EXPLICIT CACHE USED: Only dynamic content (breadcrumb + image) sent to API")
            return response, True
            
        except Exception as e:
            error_msg = str(e).lower()
            # If it's a rate limit or quota error, let the caller handle retry
            if any(x in error_msg for x in ["429", "quota", "resource", "rate"]):
                print(f"   ⚠️  Cache generate hit rate limit: {str(e)[:80]}")
                raise  # Re-raise so caller's retry logic handles it
            
            print(f"   ⚠️  Explicit cache generate failed: {str(e)[:100]}")
            print(f"   Falling back to standard (non-cached) API call.")
            
            # Invalidate cache if it seems corrupted
            cache_key = self._get_cache_key(api_key, model_name)
            if cache_key in self._cache_store:
                del self._cache_store[cache_key]
            
            return None, False
    
    def _get_or_create_cache(self, client, cache_key: str, model_name: str,
                              cacheable_content_parts: list) -> str | None:
        """
        Returns cache_name if cache exists and is valid, or creates a new one.
        Returns None if cache creation fails.
        """
        # Check for existing cache
        if cache_key in self._cache_store:
            cache_info = self._cache_store[cache_key]
            cache_age_minutes = (time.time() - cache_info['created_at']) / 60
            
            try:
                # Verify cache still exists on the server
                client.caches.get(name=cache_info['cache_name'])
                
                print(f"   🔄 EXPLICIT CACHE REUSED: '{cache_info['cache_name'][-30:]}...' "
                      f"(age: {cache_age_minutes:.1f} min)")
                return cache_info['cache_name']
                
            except Exception:
                print(f"   ⚠️  Cache expired/invalid. Recreating...")
                del self._cache_store[cache_key]
        
        # === CREATE NEW CACHE ===
        try:
            print(f"\n{'='*80}")
            print(f"🆕 CREATING EXPLICIT CACHE for model '{model_name}'")
            print(f"   Uploading static classification rules to Gemini cache server...")
            
            t_start = time.time()
            
            # Convert old SDK parts format to new SDK format
            cache_contents = self._convert_old_parts_to_new(cacheable_content_parts)
            
            # Create the cache using new google-genai SDK
            cache = client.caches.create(
                model=model_name,
                config=types.CreateCachedContentConfig(
                    contents=cache_contents,
                    ttl="3600s",  # 1 hour
                    display_name=f'awacs_truck_classification_rules',
                )
            )
            
            creation_time = time.time() - t_start
            self._cache_creation_count += 1
            
            # Store cache reference
            self._cache_store[cache_key] = {
                'cache_name': cache.name,
                'created_at': time.time()
            }
            
            # Get token info from cache metadata
            cache_usage = getattr(cache, 'usage_metadata', None)
            if cache_usage:
                token_count = getattr(cache_usage, 'total_token_count', 'unknown')
            else:
                token_count = 'unknown'
            
            print(f"   ✅ CACHE CREATED in {creation_time:.1f}s!")
            print(f"   📦 Cache Name: {cache.name}")
            print(f"   📊 Cached Tokens: {token_count}")
            print(f"   ⏰ TTL: 1 hour (auto-expires)")
            print(f"   💰 These tokens will now be billed at 90% discount for all subsequent calls!")
            print(f"{'='*80}\n")
            
            return cache.name
            
        except Exception as e:
            print(f"\n{'='*80}")
            print(f"⚠️  EXPLICIT CACHE CREATION FAILED: {e}")
            print(f"   Falling back to standard (non-cached) model.")
            print(f"   This is NOT an error — classification will still work, just without caching discount.")
            print(f"{'='*80}\n")
            return None
    
    def track_and_print_listing_savings(self, ad_id: str, cached_tokens: int,
                                         total_input_tokens: int, model_name: str,
                                         is_explicit_cache: bool):
        """
        Tracks cost savings from cached tokens and prints per-listing savings.
        """
        if cached_tokens <= 0:
            return
        
        price_input, price_cached = self._get_pricing(model_name)
        
        # Calculate savings for this listing
        savings_per_token = (price_input - price_cached) / 1_000_000
        listing_savings_usd = cached_tokens * savings_per_token
        listing_savings_cents = listing_savings_usd * 100
        cache_percent = (cached_tokens / total_input_tokens * 100) if total_input_tokens > 0 else 0
        
        # Accumulate totals
        self._total_cached_tokens += cached_tokens
        self._total_non_cached_tokens += max(0, total_input_tokens - cached_tokens)
        self._total_savings_usd += listing_savings_usd
        self._total_listings_with_cache += 1
        
        # Print per-listing savings
        cache_type = "EXPLICIT" if is_explicit_cache else "IMPLICIT"
        print(f"   💰 [{cache_type} CACHE HIT] Ad {ad_id}: "
              f"{cached_tokens:,} of {total_input_tokens:,} tokens cached ({cache_percent:.0f}%) | "
              f"Saved {listing_savings_cents:.4f}¢ on this listing | "
              f"Running total: ${self._total_savings_usd:.4f}")
    
    def print_total_savings(self):
        """Prints a comprehensive session summary of caching savings."""
        print(f"\n{'='*80}")
        print(f"📊 EXPLICIT CACHING — SESSION SAVINGS REPORT")
        print(f"{'='*80}")
        
        if self._total_listings_with_cache == 0:
            print(f"   No cache hits recorded in this session.")
            if not CACHING_AVAILABLE:
                print(f"   💡 Install SDK: pip install google-genai")
            print(f"{'='*80}\n")
            return
        
        total_savings_cents = self._total_savings_usd * 100
        avg_savings_per_listing = total_savings_cents / self._total_listings_with_cache
        total_tokens_processed = self._total_cached_tokens + self._total_non_cached_tokens
        overall_cache_rate = (self._total_cached_tokens / total_tokens_processed * 100) if total_tokens_processed > 0 else 0
        
        print(f"   🏗️  Caches Created:         {self._cache_creation_count}")
        print(f"   📋 Listings with Cache Hit: {self._total_listings_with_cache}")
        print(f"   📊 Total Cached Tokens:     {self._total_cached_tokens:,}")
        print(f"   📊 Total Non-Cached Tokens: {self._total_non_cached_tokens:,}")
        print(f"   🎯 Overall Cache Hit Rate:  {overall_cache_rate:.1f}%")
        print(f"")
        print(f"   💰 TOTAL COST SAVED:        ${self._total_savings_usd:.4f} ({total_savings_cents:.4f}¢)")
        print(f"   💰 Avg Saving Per Listing:  {avg_savings_per_listing:.4f}¢")
        print(f"")
        
        # Show what it would have cost without caching
        would_have_cost = self._total_cached_tokens / 1_000_000 * 0.30
        actually_cost = self._total_cached_tokens / 1_000_000 * 0.03
        discount_pct = (self._total_savings_usd / would_have_cost * 100) if would_have_cost > 0 else 0
        
        print(f"   💡 Without caching, these {self._total_cached_tokens:,} tokens would have cost:")
        print(f"      Regular price: ${would_have_cost:.4f}")
        print(f"      Cached price:  ${actually_cost:.4f}")
        print(f"      You saved:     ${self._total_savings_usd:.4f} ({discount_pct:.0f}% discount)")
        print(f"{'='*80}\n")
    
    def reset_tracking(self):
        """Reset all cost tracking counters."""
        self._total_cached_tokens = 0
        self._total_non_cached_tokens = 0
        self._total_savings_usd = 0.0
        self._total_listings_with_cache = 0


# === GLOBAL SINGLETON ===
_global_cache_manager = None

def get_cache_manager() -> ExplicitCacheManager:
    """Returns the global ExplicitCacheManager singleton."""
    global _global_cache_manager
    if _global_cache_manager is None:
        _global_cache_manager = ExplicitCacheManager()
    return _global_cache_manager