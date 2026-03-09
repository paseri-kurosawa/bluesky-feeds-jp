"""
Density-based content quality scoring module using .ftz model.

Simple, lightweight scoring:
1. zlib compression ratio check (noise detection)
2. Word vector norm extraction from .ftz model
3. Attribute-based adjustments (reply, images, hashtags)
"""

import zlib
import os
import re
import math
import json
from typing import List, Tuple, Dict, Any

# Global scope for warm starts
_ft_model = None
_janome_tokenizer = None
_config = None

def get_fasttext_model():
    """Lazy load fastText .ftz model (pre-cached in Lambda image)"""
    global _ft_model
    if _ft_model is None:
        import fasttext
        import urllib.request

        # Use language detection model for word vectors (lightweight)
        model_path = "/tmp/lid.176.ftz"
        if not os.path.exists(model_path):
            print("[FASTTEXT_LOAD] Downloading lid.176.ftz...")
            urllib.request.urlretrieve(
                "https://dl.fbaipublicfiles.com/fasttext/supervised-models/lid.176.ftz",
                model_path
            )

        print("[FASTTEXT_LOAD] Loading .ftz model...")
        _ft_model = fasttext.load_model(model_path)
        print("[FASTTEXT_LOAD] Model loaded successfully")

    return _ft_model

def get_janome_tokenizer():
    """Lazy load Janome tokenizer (cached for warm starts)"""
    global _janome_tokenizer
    if _janome_tokenizer is None:
        from janome.tokenizer import Tokenizer
        print("[JANOME_LOAD] Initializing Janome tokenizer...")
        _janome_tokenizer = Tokenizer()
        print("[JANOME_LOAD] Tokenizer loaded successfully")
    return _janome_tokenizer

def load_config() -> Dict[str, Any]:
    """Load scoring configuration from config.json"""
    global _config
    if _config is None:
        config_path = os.path.join(os.path.dirname(__file__), "config.json")
        print(f"[CONFIG_LOAD] Loading config from {config_path}")
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                _config = json.load(f)
            print("[CONFIG_LOAD] Config loaded successfully")
        except Exception as e:
            print(f"[CONFIG_ERROR] Failed to load config: {e}")
            # Fallback to hardcoded defaults
            _config = {
                "scoring": {
                    "sigmoid": {"midpoint": 8.0, "steepness": 0.5},
                    "compression_ratio": {"min": 0.2, "max": 0.95},
                    "attributes": {
                        "reply": {"enabled": True, "adjustment": 0.5, "operation": "multiply"},
                        "images": {"enabled": True, "adjustment": 1.0, "operation": "add"},
                        "hashtags": {
                            "enabled": True,
                            "rules": [
                                {"min": 1, "max": 2, "adjustment": 1.0, "operation": "add"},
                                {"min": 3, "max": 4, "adjustment": 0.0, "operation": "add"},
                                {"min": 5, "max": None, "adjustment": 1.0, "operation": "subtract"}
                            ]
                        }
                    }
                }
            }
    return _config

def calculate_compression_ratio(text: str) -> float:
    """
    Calculate zlib compression ratio.

    Args:
        text: Input text

    Returns:
        Compression ratio (compressed_size / original_size)
    """
    if not text:
        return 1.0

    original_bytes = text.encode('utf-8')
    compressed_bytes = zlib.compress(original_bytes, level=9)

    ratio = len(compressed_bytes) / len(original_bytes)
    return ratio

def tokenize_japanese(text: str) -> List[str]:
    """
    Japanese tokenization using Janome morphological analyzer.

    Extracts content words only:
    - 名詞 (nouns)
    - 動詞 (verbs)
    - 形容詞 (adjectives)
    - 副詞 (adverbs)

    Filters out:
    - Particles (助詞)
    - Auxiliary verbs (助動詞)
    - Symbols (記号)
    - Non-content nouns: pronouns, numbers, suffixes, non-independent nouns

    Args:
        text: Input text

    Returns:
        List of content word surface forms
    """
    tokenizer = get_janome_tokenizer()
    content_words = []

    # Target POS tags (part-of-speech)
    target_pos_prefixes = ('名詞', '動詞', '形容詞', '副詞')

    # Exclude specific noun subtypes that aren't content-bearing
    exclude_pos_patterns = (
        '名詞,非自立',      # Non-independent nouns
        '名詞,代名詞',      # Pronouns
        '名詞,数',          # Numbers
        '名詞,接尾',        # Suffixes
    )

    try:
        for token in tokenizer.tokenize(text):
            # token.part_of_speech: "品詞,品詞細分類1,品詞細分類2,..."
            # token.surface: surface form (表層形)
            pos = token.part_of_speech
            surface = token.surface

            # Skip empty or whitespace-only tokens
            if not surface or not surface.strip():
                continue

            # Check if POS starts with target categories
            if not any(pos.startswith(prefix) for prefix in target_pos_prefixes):
                continue

            # Exclude non-content noun subtypes
            if any(pos.startswith(pattern) for pattern in exclude_pos_patterns):
                continue

            # Length filter: exclude very short tokens (1 character)
            # Exception: kanji single characters are often meaningful
            if len(surface) == 1 and not ('\u4e00' <= surface <= '\u9fff'):
                continue

            content_words.append(surface)

    except Exception as e:
        print(f"[TOKENIZE_ERROR] Janome error: {e}")
        import traceback
        traceback.print_exc()
        # Fallback: return empty list (will result in score=0)
        return []

    print(f"[JANOME_TOKENS] Extracted {len(content_words)} content words")
    return content_words

def extract_word_vector_norms(text: str) -> List[Tuple[str, float]]:
    """
    Extract word vector norms using .ftz model.

    Args:
        text: Input text

    Returns:
        List of (word, vector_norm) tuples
    """
    ft_model = get_fasttext_model()
    tokens = tokenize_japanese(text)
    words_with_norms = []

    try:
        for token_idx, token in enumerate(tokens):
            # Debug: log first 5 tokens
            if token_idx < 5:
                try:
                    vector = ft_model.get_word_vector(token)
                    norm = float((vector ** 2).sum() ** 0.5)
                    print(f"[TOKEN_DEBUG] idx={token_idx}, text='{token}', vector_norm={norm:.4f}")
                except Exception as e:
                    print(f"[TOKEN_DEBUG] idx={token_idx}, text='{token}', error={e}")

            # Get word vector and compute norm
            try:
                vector = ft_model.get_word_vector(token)
                norm = float((vector ** 2).sum() ** 0.5)
                if norm > 0:
                    words_with_norms.append((token, norm))
            except Exception as e:
                print(f"[VECTOR_ERROR] token='{token}': {e}")

    except Exception as e:
        print(f"[ERROR] Error in extract_word_vector_norms: {e}")
        import traceback
        traceback.print_exc()
        return []

    print(f"[VECTORS_EXTRACTED] {len(words_with_norms)} tokens with vectors out of {len(tokens)}")
    return words_with_norms


def apply_attribute_adjustments(avg_norm: float, is_reply: bool, has_images: bool, hashtag_count: int) -> Tuple[float, str]:
    """
    Apply attribute-based adjustments to avg_norm before sigmoid normalization.
    Adjustments are loaded from config.json.

    Args:
        avg_norm: Average word vector norm
        is_reply: Whether post is a reply
        has_images: Whether post has images
        hashtag_count: Number of hashtags (from record.facets)

    Returns:
        Tuple of (adjusted_norm, adjustment_log)
    """
    config = load_config()
    attr_config = config["scoring"]["attributes"]
    adjusted_norm = avg_norm
    adjustments = []

    # Reply adjustment
    if is_reply and attr_config["reply"]["enabled"]:
        reply_conf = attr_config["reply"]
        if reply_conf["operation"] == "multiply":
            adjusted_norm *= reply_conf["adjustment"]
            adjustments.append(f"reply:×{reply_conf['adjustment']}")
        elif reply_conf["operation"] == "add":
            adjusted_norm += reply_conf["adjustment"]
            adjustments.append(f"reply:+{reply_conf['adjustment']}")

    # Images adjustment
    if has_images and attr_config["images"]["enabled"]:
        img_conf = attr_config["images"]
        if img_conf["operation"] == "add":
            adjusted_norm += img_conf["adjustment"]
            adjustments.append(f"images:+{img_conf['adjustment']}")

    # Hashtags adjustment (rule-based)
    if hashtag_count > 0 and attr_config["hashtags"]["enabled"]:
        for rule in attr_config["hashtags"]["rules"]:
            rule_min = rule["min"]
            rule_max = rule["max"]

            # Check if hashtag_count matches this rule
            if rule_max is None:
                # No upper limit
                if hashtag_count >= rule_min:
                    if rule["operation"] == "add":
                        adjusted_norm += rule["adjustment"]
                        if rule["adjustment"] != 0:
                            adjustments.append(f"hashtags({hashtag_count}):+{rule['adjustment']}")
                        else:
                            adjustments.append(f"hashtags({hashtag_count}):±0.0")
                    elif rule["operation"] == "subtract":
                        adjusted_norm -= rule["adjustment"]
                        adjustments.append(f"hashtags({hashtag_count}):−{rule['adjustment']}")
                    break
            else:
                # Both min and max specified
                if rule_min <= hashtag_count <= rule_max:
                    if rule["operation"] == "add":
                        adjusted_norm += rule["adjustment"]
                        if rule["adjustment"] != 0:
                            adjustments.append(f"hashtags({hashtag_count}):+{rule['adjustment']}")
                        else:
                            adjustments.append(f"hashtags({hashtag_count}):±0.0")
                    break

    adjustment_log = ", ".join(adjustments) if adjustments else "none"
    return adjusted_norm, adjustment_log

def calculate_density_score(text: str, is_reply: bool = False, has_images: bool = False, hashtag_count: int = 0) -> float:
    """
    Calculate density score for a text with attribute adjustments.

    Algorithm:
    - Step 1: Check zlib compression ratio (noise detection)
    - Step 2: Extract word vectors using .ftz model
    - Step 3: Calculate average vector norm
    - Step 4: Apply attribute adjustments (before sigmoid normalization)
    - Step 5: Normalize to 0-1 scale using sigmoid

    Args:
        text: Input text
        is_reply: Whether post is a reply (default: False)
        has_images: Whether post has images (default: False)
        hashtag_count: Number of hashtags from record.facets (default: 0)

    Returns:
        Density score (0-1 scale), or 0 if text fails checks
    """
    try:
        # Step 1: Compression ratio check
        config = load_config()
        comp_conf = config["scoring"]["compression_ratio"]
        comp_min = comp_conf["min"]
        comp_max = comp_conf["max"]

        comp_ratio = calculate_compression_ratio(text)

        # Noise detection: ratio too low (random) or too high (repetitive)
        if comp_ratio < comp_min or comp_ratio > comp_max:
            print(f"[DENSITY] Failed compression check (ratio={comp_ratio:.3f}, min={comp_min}, max={comp_max})")
            return 0.0

        # Step 2: Extract word vectors
        words_with_norms = extract_word_vector_norms(text)

        if not words_with_norms:
            print("[DENSITY] No word vectors found")
            return 0.0

        # Step 3: Calculate average norm
        norms = [norm for _, norm in words_with_norms]
        avg_norm = sum(norms) / len(norms)
        min_norm = min(norms)
        max_norm = max(norms)

        # Step 4: Apply attribute adjustments
        adjusted_norm, adjustment_log = apply_attribute_adjustments(avg_norm, is_reply, has_images, hashtag_count)

        # Step 5: Normalize to 0-1 scale using sigmoid
        # Load sigmoid parameters from config
        config = load_config()
        sigmoid_conf = config["scoring"]["sigmoid"]
        sigmoid_midpoint = sigmoid_conf["midpoint"]
        sigmoid_steepness = sigmoid_conf["steepness"]
        normalized_score = 1.0 / (1.0 + math.exp(-sigmoid_steepness * (adjusted_norm - sigmoid_midpoint)))

        # Clamp score to 0-1 range (safety check)
        normalized_score = max(0.0, min(1.0, normalized_score))

        # Detailed analysis log
        print(f"[VECTOR_ANALYSIS] min={min_norm:.4f}, max={max_norm:.4f}, avg={avg_norm:.4f}, token_count={len(words_with_norms)}")
        print(f"[DENSITY_SCORE] {normalized_score:.3f} (avg_norm={avg_norm:.4f}→{adjusted_norm:.4f}, adjustments=[{adjustment_log}], comp_ratio={comp_ratio:.3f})")
        return normalized_score

    except Exception as e:
        print(f"[DENSITY_ERROR] Error: {e}")
        import traceback
        traceback.print_exc()
        return 0.0
