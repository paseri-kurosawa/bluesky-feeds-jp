"""
Density-based content quality scoring module using .ftz model.

Simple, lightweight scoring:
1. Token dispersion check (repetition detection)
2. Word vector norm extraction from .ftz model
3. Attribute-based adjustments (reply, images, hashtags)
"""

import os
import re
import math
import json
import boto3
from typing import List, Tuple, Dict, Any

# Global scope for warm starts
_ft_model = None
_janome_tokenizer = None
_config = None
_badwords_config = None
_s3_client = None

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
            raise  # No fallback - config must be present
    return _config

def load_badwords_config() -> Dict[str, Any]:
    """
    Load badword dictionary from S3 (text format: 1 word per line).

    【仕様】
    ファイル形式: S3 の badwords/dictionary.txt
    内容: UTF-8 テキスト、1行1単語（見出し語ベース）
    エンコーディング: UTF-8
    マッチング方式: 見出し語（基本形）ベース、case-insensitive

    【重要】
    - Janome で形態素解析した見出し語と比較
    - 活用形は自動的に見出し語に正規化される
    - 例: 『殺す』『殺そう』『殺した』 → 全て『殺す』の見出し語でマッチ
    - 例: 『言う』『言った』『言い張る』 → 対応する見出し語でマッチ

    【ペナルティ】
    - 固定値: 0.75（25% 減衰）
    - 複数バッドワード時: adjusted_norm *= (0.75 ^ count)
    - 1つ: 0.75倍（25% 減衰）
    - 2つ: 0.5625倍（43.75% 減衰）
    - 3つ: 0.4219倍（57.81% 減衰）

    【適用段階】
    - Step 4.4: Hashtags 調整の直後、シグモイド正規化前に適用
    """
    global _badwords_config
    if _badwords_config is None:
        s3_bucket = os.environ.get("S3_BUCKET", "")

        if not s3_bucket:
            print("[BADWORDS_LOAD] S3_BUCKET not set, badwords disabled")
            _badwords_config = {"badwords": [], "penalty": 0.75}
            return _badwords_config

        try:
            global _s3_client
            if _s3_client is None:
                _s3_client = boto3.client("s3")

            print(f"[BADWORDS_LOAD] Loading from s3://{s3_bucket}/badwords/dictionary.txt")
            response = _s3_client.get_object(
                Bucket=s3_bucket,
                Key="badwords/dictionary.txt"
            )

            # Read text format: 1 word per line (UTF-8)
            # Empty lines are automatically filtered out
            text_content = response["Body"].read().decode("utf-8")
            badwords = [word.strip() for word in text_content.split("\n") if word.strip()]

            _badwords_config = {
                "badwords": badwords,
                "penalty": 0.75  # Fixed penalty: 25% attenuation per badword
            }

            badword_count = len(badwords)
            print(f"[BADWORDS_LOAD] Loaded {badword_count} badwords (text format, 1 word per line, base form matching)")

        except Exception as e:
            print(f"[BADWORDS_LOAD] Failed to load badwords: {e}")
            # Fallback: empty badwords (badword filtering disabled)
            _badwords_config = {"badwords": [], "penalty": 0.75}

    return _badwords_config

def calculate_token_dispersion(text: str) -> float:
    """
    Calculate token dispersion score (0-1).

    Higher values indicate more natural language with diverse vocabulary.
    Lower values indicate repetitive, unnatural text.

    Formula: unique_tokens / total_tokens
    - 1.0 = all unique (most natural)
    - 0.0 = no tokens or complete repetition (least natural)

    Args:
        text: Input text

    Returns:
        Dispersion score (0-1)
    """
    tokens = tokenize_japanese(text)
    if not tokens:
        return 0.0

    unique_count = len(set(tokens))
    total_count = len(tokens)
    dispersion = unique_count / total_count

    return dispersion

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

def extract_base_forms(text: str) -> List[str]:
    """
    Extract base forms (見出し語) from Japanese text using Janome.

    Same filtering as tokenize_japanese, but returns base forms instead of surface forms.
    For example: 「殺そう」→ 「殺す」（base form）

    Args:
        text: Input text

    Returns:
        List of base forms (見出し語)
    """
    tokenizer = get_janome_tokenizer()
    base_forms = []

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
            pos = token.part_of_speech
            surface = token.surface
            base = token.base_form  # 見出し語（基本形）

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

            base_forms.append(base)

    except Exception as e:
        print(f"[EXTRACT_BASE_FORMS_ERROR] Janome error: {e}")
        import traceback
        traceback.print_exc()
        return []

    print(f"[EXTRACT_BASE_FORMS] Extracted {len(base_forms)} base forms")
    return base_forms

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


def count_badwords_in_tokens(tokens: List[str]) -> Tuple[int, List[str]]:
    """
    Count how many badwords appear in the token list, and return matched words.

    【重要な仕様】
    - tokens: Janome で形態素解析された見出し語（基本形）のリスト
    - マッチング方式: 大文字小文字を区別しない（case-insensitive）
    - 辞書: S3 から読み込まれた badwords/dictionary.txt（1行1単語、見出し語ベース）

    【動作】
    各トークン（見出し語）が辞書に含まれるかチェック → マッチ数と該当ワード一覧を返す

    Args:
        tokens: List of base forms (見出し語) from Janome morphological analysis

    Returns:
        Tuple of (badword_count, matched_words_list)
        - badword_count: Number of badword matches found
        - matched_words_list: List of matched badwords (with original capitalization from dictionary)
    """
    badwords_config = load_badwords_config()
    badwords = badwords_config.get("badwords", [])

    if not badwords:
        return 0, []

    # Create lowercase mapping: lowercase -> original
    badwords_lower_map = {word.lower(): word for word in badwords}
    tokens_lower = [token.lower() for token in tokens]

    # Find matches with original capitalization
    matched_words = []
    for token in tokens_lower:
        if token in badwords_lower_map:
            matched_words.append(badwords_lower_map[token])

    badword_count = len(matched_words)
    return badword_count, matched_words

def apply_attribute_adjustments(avg_norm: float, is_reply: bool, has_images: bool, hashtag_count: int, tokens: List[str] = None) -> Tuple[float, str]:
    """
    Apply attribute-based adjustments to avg_norm before sigmoid normalization.
    Adjustments are loaded from config.json.

    Args:
        avg_norm: Average word vector norm
        is_reply: Whether post is a reply
        has_images: Whether post has images
        hashtag_count: Number of hashtags (from record.facets)
        tokens: List of tokenized words for badword checking (optional)

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

    # Step 4.4: Badword adjustment（Hashtags の直後）
    # 【仕様】
    # - 見出し語ベースのバッドワードマッチング（活用形は自動正規化）
    # - ペナルティ: 0.75（25% 減衰）
    # - 複数マッチ時: 乗算的に適用 adjusted_norm *= (0.75 ^ count)
    # - 例: 2つのバッドワード → adjusted_norm *= 0.75^2 = 0.5625（43.75% 減衰）
    if tokens:
        badword_count, matched_words = count_badwords_in_tokens(tokens)
        if badword_count > 0:
            badwords_config = load_badwords_config()
            penalty = badwords_config.get("penalty", 0.75)
            # Apply penalty multiplicatively: adjusted_norm *= penalty^badword_count
            # 複数のバッドワードが含まれる場合、ペナルティは指数的に適用される
            multiplier = penalty ** badword_count
            adjusted_norm *= multiplier
            matched_words_str = "、".join(matched_words)
            adjustments.append(f"badwords({badword_count}):×{multiplier:.4f}")
            print(f"[BADWORD_PENALTY] Found {badword_count} badword(s): 【{matched_words_str}】, penalty multiplier={multiplier:.4f} (0.75^{badword_count})")

    adjustment_log = ", ".join(adjustments) if adjustments else "none"
    return adjusted_norm, adjustment_log

def calculate_density_score(text: str, is_reply: bool = False, has_images: bool = False, hashtag_count: int = 0) -> Tuple[float, int, List[str]]:
    """
    Calculate density score for a text with attribute adjustments.

    Algorithm:
    - Step 0: Tokenize text once (cached for reuse)
    - Step 1: Check token dispersion (repetition detection)
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
        Tuple of (density_score, badword_count, matched_words):
        - density_score: Density score (0-1 scale), or 0 if text fails checks
        - badword_count: Number of badwords matched in the text
        - matched_words: List of matched badwords
    """
    try:
        # Step 0: Tokenize text once and cache for reuse
        tokens = tokenize_japanese(text)
        base_forms = extract_base_forms(text)

        # Step 1: Token dispersion check (repetition detection)
        config = load_config()
        disp_conf = config["scoring"]["token_dispersion"]
        disp_threshold = disp_conf["threshold"]

        if not tokens:
            # No tokens extracted = noise
            print(f"[DENSITY] No tokens extracted (likely noise)")
            return 0.0, 0, []

        dispersion = len(set(tokens)) / len(tokens)

        if dispersion < disp_threshold:
            print(f"[DENSITY] Failed token dispersion check (dispersion={dispersion:.3f}, BELOW_THRESHOLD: {disp_threshold})")
            return 0.0, 0, []

        # Step 2: Extract word vectors
        words_with_norms = extract_word_vector_norms(text)

        if not words_with_norms:
            print("[DENSITY] No word vectors found")
            return 0.0, 0, []

        # Step 3: Calculate average norm
        norms = [norm for _, norm in words_with_norms]
        avg_norm = sum(norms) / len(norms)
        min_norm = min(norms)
        max_norm = max(norms)

        # Count badwords for statistics (using cached base_forms)
        badword_count, matched_words = count_badwords_in_tokens(base_forms)

        # Step 4: Apply attribute adjustments (including badword penalty, using cached tokens)
        adjusted_norm, adjustment_log = apply_attribute_adjustments(avg_norm, is_reply, has_images, hashtag_count, tokens)

        # Step 5: Normalize to 0-1 scale using sigmoid
        sigmoid_conf = config["scoring"]["sigmoid"]
        sigmoid_midpoint = sigmoid_conf["midpoint"]
        sigmoid_steepness = sigmoid_conf["steepness"]
        normalized_score = 1.0 / (1.0 + math.exp(-sigmoid_steepness * (adjusted_norm - sigmoid_midpoint)))

        # Clamp score to 0-1 range (safety check)
        normalized_score = max(0.0, min(1.0, normalized_score))

        # Detailed analysis log
        print(f"[VECTOR_ANALYSIS] min={min_norm:.4f}, max={max_norm:.4f}, avg={avg_norm:.4f}, token_count={len(words_with_norms)}")
        print(f"[DENSITY_SCORE] {normalized_score:.3f} (avg_norm={avg_norm:.4f}→{adjusted_norm:.4f}, adjustments=[{adjustment_log}], dispersion={dispersion:.3f})")
        return normalized_score, badword_count, matched_words

    except Exception as e:
        print(f"[DENSITY_ERROR] Error: {e}")
        import traceback
        traceback.print_exc()
        return 0.0, 0, []
